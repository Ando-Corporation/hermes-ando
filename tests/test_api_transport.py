import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_invitation import VALUE


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"), "Set HERMES_SOURCE for real MCP transport tests"
)
class ApiTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_invite_key_uses_mcp_bearer_and_public_api_ticket_without_oauth(self):
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        from tools.mcp_tool import sdk_httpx
        from plugin.transport import AndoTransport

        httpx = sdk_httpx()
        seen = []

        async def handle(request):
            seen.append(str(request.url))
            self.assertEqual(
                request.headers["authorization"], "Bearer test-private-key"
            )
            if request.url.host == "api.ando.so":
                self.assertEqual(request.url.path, "/v1/realtime/connections")
                self.assertEqual(request.headers["x-api-key"], "test-private-key")
                body = json.loads(request.content)
                self.assertEqual(body["subscriptions"][0]["target"], "self")
                return httpx.Response(
                    200,
                    json={
                        "url": "wss://realtime.ando.so/link?ticket=private",
                        "protocol": "ando.realtime.v1",
                        "resume_cursor": "floor",
                        "subscriptions": [
                            {
                                "target": {
                                    "id": VALUE["agent_membership_id"],
                                    "type": "workspace_membership",
                                }
                            }
                        ],
                    },
                )
            self.assertEqual(str(request.url), VALUE["mcp_url"])
            if request.method in {"GET", "DELETE"}:
                return httpx.Response(405)
            body = json.loads(request.content)
            if "id" not in body:
                return httpx.Response(202)
            if body["method"] == "initialize":
                result = {
                    "protocolVersion": body["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "Ando", "version": "1"},
                }
            elif body["method"] == "tools/list":
                result = {"tools": []}
            else:
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {"identity": {"identity_type": "agent"}}
                            ),
                        }
                    ]
                }
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": body["id"], "result": result}
            )

        real = httpx.AsyncClient
        with patch.object(
            httpx,
            "AsyncClient",
            side_effect=lambda **kw: real(**kw, transport=httpx.MockTransport(handle)),
        ):
            transport = AndoTransport(
                None, VALUE["agent_membership_id"], connection=VALUE
            )
            async with transport.connected():
                result = await transport.call("get_current_identity", {})
                self.assertEqual(result["identity"]["identity_type"], "agent")
                await transport.ticket(None)
        self.assertTrue(any("/v1/realtime/connections" in url for url in seen))
        self.assertFalse(any("oauth" in url for url in seen))


class ReplayRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_replay_subscribes_before_inbox_recovery(self):
        from plugin.transport import ReplayExpired, listen
        from unittest.mock import AsyncMock, MagicMock
        calls = []
        async def ticket(cursor):
            calls.append(cursor)
            if cursor:
                raise ReplayExpired("expired")
            return {"url": "wss://example.invalid", "resume_cursor": "fresh"}
        transport = MagicMock(connection={"credential_mode": "invitation"})
        transport.ticket = ticket
        state = MagicMock()
        state.get.return_value = "old"
        socket = MagicMock(subprotocol="ando.realtime.v1")
        socket.__aiter__.return_value = iter([])
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=socket)
        context.__aexit__ = AsyncMock(return_value=False)
        order = []
        async def recover():
            order.append("recover")
        with patch("plugin.transport.websocket_connect", return_value=context):
            await listen(transport, state, AsyncMock(), lambda: order.append("connected"), recover)
        self.assertEqual(calls, ["old", None])
        self.assertEqual(order, ["connected", "recover"])
        state.set.assert_called_once_with("cursor", "fresh")

    async def test_no_inbox_or_no_invitation_does_not_skip_expired_replay(self):
        from plugin.transport import ReplayExpired, listen
        from unittest.mock import AsyncMock, MagicMock
        for connection, recover in [({}, None), (None, AsyncMock())]:
            transport = MagicMock(connection=connection)
            transport.ticket = AsyncMock(side_effect=ReplayExpired("expired"))
            state = MagicMock()
            with self.assertRaises(ReplayExpired):
                await listen(transport, state, AsyncMock(), lambda: None, recover)
            self.assertEqual(transport.ticket.await_count, 1)
            state.set.assert_not_called()

    async def test_authorization_failure_never_retries_without_cursor(self):
        from plugin.transport import listen
        from plugin.protocol import SetupRequired
        from unittest.mock import AsyncMock, MagicMock
        transport = MagicMock(connection={})
        transport.ticket = AsyncMock(side_effect=SetupRequired("revoked"))
        state = MagicMock()
        with self.assertRaises(SetupRequired):
            await listen(transport, state, AsyncMock(), lambda: None, AsyncMock())
        self.assertEqual(transport.ticket.await_count, 1)
        state.set.assert_not_called()

    async def test_only_explicit_server_replay_expiry_is_classified(self):
        from plugin.transport import AndoTransport, ReplayExpired
        from plugin.protocol import SetupRequired
        from unittest.mock import AsyncMock, MagicMock
        transport = AndoTransport(None, VALUE["agent_membership_id"], connection=VALUE)
        response = MagicMock(status_code=400)
        response.json.return_value = {"error": {"code": "invalid_request", "message":
            "Realtime resume cursor is outside the 24-hour replay window. Reconnect without resume_from."}}
        transport.http = MagicMock(post=AsyncMock(return_value=response))
        with self.assertRaises(ReplayExpired):
            await transport.ticket("old")
        for status, message in [(401, "revoked"), (400, "invalid subscription")]:
            response.status_code = status
            response.json.return_value = {"error": {"code": "invalid_request", "message": message}}
            with self.assertRaises(SetupRequired) as result:
                await transport.ticket("old")
            self.assertNotIsInstance(result.exception, ReplayExpired)

    async def test_malformed_replay_error_keeps_explicit_stop(self):
        from plugin.transport import AndoTransport, ReplayExpired
        from plugin.protocol import SetupRequired
        from unittest.mock import AsyncMock, MagicMock
        transport = AndoTransport(None, VALUE["agent_membership_id"], connection=VALUE)
        for body in [None, [], {"error": None}, {"error": {"message": None}}, ValueError("non-JSON")]:
            response = MagicMock(status_code=400)
            if isinstance(body, Exception):
                response.json.side_effect = body
            else:
                response.json.return_value = body
            transport.http = MagicMock(post=AsyncMock(return_value=response))
            with self.assertRaises(SetupRequired) as result:
                await transport.ticket("old")
            self.assertNotIsInstance(result.exception, ReplayExpired)
