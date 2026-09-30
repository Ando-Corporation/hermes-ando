import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE for actual Hermes OAuth/MCP tests",
)
class OAuthTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_hermes_token_refreshes_before_mcp_and_ticket_requests(self):
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        from mcp.shared.auth import (
            OAuthClientInformationFull,
            OAuthMetadata,
            OAuthToken,
        )
        from tools.mcp_oauth import HermesTokenStorage
        from tools.mcp_oauth_manager import reset_manager_for_tests
        from tools.mcp_tool import sdk_httpx
        from plugin.transport import AndoTransport

        httpx = sdk_httpx()
        requests = []
        scoped_url = "https://mcp.ando.so/mcp/pair/test-only-issuance"

        async def handle(request):
            requests.append(
                (request.method, str(request.url), request.headers.get("authorization"))
            )
            if request.url.path == "/oauth/token":
                return httpx.Response(
                    200,
                    json={
                        "access_token": "fixture-refreshed",
                        "refresh_token": "fixture-next",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    },
                )
            if request.url.path == "/realtime/connections":
                self.assertEqual(
                    request.headers["authorization"], "Bearer fixture-refreshed"
                )
                self.assertEqual(
                    json.loads(request.content)["resume_from"], {"cursor": "safe"}
                )
                return httpx.Response(
                    200,
                    json={
                        "url": "wss://mcp.ando.so/link?ticket=fixture",
                        "protocol": "ando.realtime.v1",
                        "resume_cursor": "safe",
                        "subscriptions": [
                            {"target": {"id": "agent", "type": "workspace_membership"}}
                        ],
                    },
                )
            self.assertEqual(str(request.url), scoped_url)
            self.assertEqual(
                request.headers["authorization"], "Bearer fixture-refreshed"
            )
            if request.method == "GET":
                return httpx.Response(405)
            if request.method == "DELETE":
                return httpx.Response(200)
            body = json.loads(request.content)
            if "id" not in body:
                return httpx.Response(202)
            if body["method"] == "initialize":
                result = {
                    "protocolVersion": body["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "Ando fixture", "version": "1"},
                }
            elif body["method"] == "tools/call":
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {"identity": {"identity_type": "agent"}}
                            ),
                        }
                    ],
                    "isError": False,
                }
            elif body["method"] == "tools/list":
                result = {
                    "tools": [
                        {
                            "name": "get_current_identity",
                            "inputSchema": {"type": "object"},
                        }
                    ]
                }
            else:
                raise AssertionError(body["method"])
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": body["id"], "result": result}
            )

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.dict(os.environ, {"HERMES_HOME": tmp}),
        ):
            reset_manager_for_tests()
            storage = HermesTokenStorage("ando-fixture")
            await storage.set_tokens(
                OAuthToken(
                    access_token="fixture-expired",
                    refresh_token="fixture-refresh",
                    token_type="Bearer",
                    expires_in=0,
                )
            )
            await storage.set_client_info(
                OAuthClientInformationFull(
                    client_id="fixture-client",
                    redirect_uris=["http://localhost:8765/callback"],
                    token_endpoint_auth_method="none",
                )
            )
            storage.save_oauth_metadata(
                OAuthMetadata(
                    issuer="https://mcp.ando.so",
                    authorization_endpoint="https://mcp.ando.so/oauth/authorize",
                    token_endpoint="https://mcp.ando.so/oauth/token",
                    response_types_supported=["code"],
                )
            )
            server = {"url": scoped_url, "auth": "oauth", "enabled": False}
            real_client = httpx.AsyncClient

            def client(**kwargs):
                return real_client(**kwargs, transport=httpx.MockTransport(handle))

            with (
                patch("plugin.transport.configured_server", return_value=server),
                patch.object(httpx, "AsyncClient", side_effect=client),
            ):
                transport = AndoTransport("ando-fixture", "agent")
                async with transport.connected():
                    result = await transport.call("get_current_identity", {})
                    self.assertEqual(result["identity"]["identity_type"], "agent")
                    await transport.ticket("safe")
            self.assertEqual(requests[0][1], "https://mcp.ando.so/oauth/token")
            self.assertTrue(any(url == scoped_url for _, url, _ in requests))
            self.assertEqual(
                (await storage.get_tokens()).access_token, "fixture-refreshed"
            )
            reset_manager_for_tests()


if __name__ == "__main__":
    unittest.main()
