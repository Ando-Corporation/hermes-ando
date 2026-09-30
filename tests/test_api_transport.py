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
