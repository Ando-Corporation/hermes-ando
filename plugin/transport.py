"""Native Hermes OAuth + MCP transport and ordered Ando realtime delivery.

The exact configured MCP URL remains the OAuth resource, including its pairing
scope. Only the ticket endpoint is derived from that resource's origin. OAuth
secrets stay in Hermes's own manager/storage; none are copied into plugin state.
"""

import asyncio
from contextlib import asynccontextmanager
import json

from .protocol import PROTOCOL, SetupRequired, resource_origin, verify_ticket


def websocket_connect(*args, **kwargs):
    # Keep the optional socket dependency at the actual network boundary.
    from websockets.asyncio.client import connect
    return connect(*args, **kwargs)


class ReplayExpired(SetupRequired):
    """The server explicitly rejected only the realtime replay checkpoint."""


def configured_server(name):
    from hermes_cli.config import load_config

    server = (load_config().get("mcp_servers") or {}).get(name)
    if not isinstance(server, dict) or server.get("auth") != "oauth":
        raise SetupRequired(
            "Configure the named Ando MCP server with OAuth before starting the gateway"
        )
    if server.get("enabled") is not False:
        raise SetupRequired(
            "The Ando gateway requires a dedicated MCP entry with model discovery disabled"
        )
    resource_origin(server.get("url", ""))
    return server


class AndoTransport:
    def __init__(self, server_name, membership_id, *, connection=None):
        self.connection = connection
        self.server_name = server_name
        self.membership_id = membership_id
        self.session = None
        self.http = None

    @asynccontextmanager
    async def connected(self):
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client
        from mcp.types import Implementation

        if self.connection is not None:
            from .connection import api_origin, validate_connection
            from tools.mcp_tool import sdk_httpx

            httpx = sdk_httpx()
            validate_connection(self.connection)
            self.origin = api_origin(self.connection)
            async with httpx.AsyncClient(
                headers={"Authorization": f"Bearer {self.connection['api_key']}"},
                follow_redirects=False,
                timeout=httpx.Timeout(30.0, read=300.0),
            ) as http:
                self.http = http
                async with streamable_http_client(
                    self.connection["mcp_url"], http_client=http
                ) as streams:
                    async with ClientSession(
                        streams[0],
                        streams[1],
                        client_info=Implementation(name="Ando Hermes", version="0.2.0"),
                    ) as session:
                        self.session = session
                        await session.initialize()
                        try:
                            yield self
                        finally:
                            self.session = None
                            self.http = None
            return
        from tools.mcp_oauth import HermesTokenStorage, suppress_interactive_oauth
        from tools.mcp_oauth_manager import get_manager
        from tools.mcp_tool import sdk_httpx

        config = configured_server(self.server_name)
        if not HermesTokenStorage(self.server_name).has_cached_tokens():
            raise SetupRequired(
                "Approve Ando with the setup helper before starting the gateway"
            )
        resource_url = config["url"]
        self.origin = resource_origin(resource_url)
        # A gateway must never unexpectedly open consent or pair another agent.
        with suppress_interactive_oauth():
            provider = get_manager().get_or_build_provider(
                self.server_name, resource_url, config.get("oauth")
            )
            if provider is None:
                raise SetupRequired(
                    "Hermes MCP OAuth support is unavailable; install its MCP extra"
                )
            httpx = sdk_httpx()
            async with httpx.AsyncClient(
                auth=provider,
                follow_redirects=False,
                timeout=httpx.Timeout(30.0, read=300.0),
            ) as http:
                self.http = http
                async with streamable_http_client(
                    resource_url, http_client=http
                ) as streams:
                    # MCP 2 returns two streams; MCP 1 also returned a session-id getter.
                    read, write = streams[:2]
                    async with ClientSession(
                        read,
                        write,
                        client_info=Implementation(name="Ando Hermes", version="0.1.0"),
                    ) as session:
                        self.session = session
                        await session.initialize()
                        try:
                            yield self
                        finally:
                            self.session = None
                            self.http = None

    async def call(self, name, arguments):
        if self.session is None:
            raise RuntimeError("Ando MCP session is not connected")
        result = await self.session.call_tool(name, arguments)
        if getattr(result, "is_error", getattr(result, "isError", False)):
            # Tool error text can contain private context or URLs. Keep logs generic.
            raise RuntimeError(f"Ando {name} failed; review the connection in Ando")
        structured = getattr(
            result, "structured_content", getattr(result, "structuredContent", None)
        )
        if isinstance(structured, dict):
            return structured
        for block in result.content:
            if getattr(block, "type", None) == "text":
                try:
                    value = json.loads(block.text)
                    if isinstance(value, dict):
                        return value
                except (TypeError, ValueError):
                    continue
        raise RuntimeError(f"Ando {name} returned an unsupported response")

    async def ticket(self, cursor):
        body = {
            "subscriptions": [
                {
                    "target": "self",
                    "delivery": "messages",
                    "events": ["message.created"],
                }
            ]
        }
        if cursor:
            body["resume_from"] = {"cursor": cursor}
        from .connection import realtime_endpoint, websocket_origin

        endpoint = (
            realtime_endpoint(self.connection)
            if self.connection
            else f"{self.origin}/realtime/connections"
        )
        response = await self.http.post(
            endpoint,
            json=body,
            headers={"x-api-key": self.connection["api_key"]}
            if self.connection
            else None,
        )
        if cursor and response.status_code == 400:
            try:
                detail = response.json()
            except ValueError:
                detail = None
            error = detail.get("error") if isinstance(detail, dict) else None
            if (
                isinstance(error, dict)
                and error.get("code") == "invalid_request"
                and isinstance(error.get("message"), str)
                and error["message"].startswith(
                    "Realtime resume cursor is outside the 24-hour replay window."
                )
            ):
                raise ReplayExpired("Ando realtime replay window expired")
        if response.status_code in {400, 401, 403, 404}:
            raise SetupRequired(
                "Ando realtime authorization or replay was rejected. Reconnect explicitly; no messages were skipped."
            )
        if response.status_code != 200:
            raise RuntimeError(
                f"Ando realtime unavailable (HTTP {response.status_code})"
            )
        ticket = response.json()
        verify_ticket(
            ticket,
            websocket_origin(self.connection) if self.connection else self.origin,
            self.membership_id,
        )
        return ticket


async def receive_socket(socket, state, on_event):
    """ACK only after handling; checkpoint only server-confirmed safe cursors.

    This intentionally serializes handling across conversations. The websocket
    library responds to control-frame pings independently while Hermes works.
    """
    pending = set()
    async for raw in socket:
        try:
            frame = json.loads(raw)
        except (ValueError, TypeError):
            raise SetupRequired("Malformed Ando realtime frame") from None
        kind = frame.get("type")
        if kind == "hello":
            continue
        if kind == "event":
            envelope = frame.get("envelope_id")
            if not isinstance(envelope, str) or not envelope:
                raise SetupRequired("Ando event has no delivery envelope")
            try:
                await on_event(frame)
            except asyncio.CancelledError:
                raise
            except Exception:
                await socket.send(
                    json.dumps(
                        {"envelope_id": envelope, "error": {"code": "handler_failed"}}
                    )
                )
                raise
            pending.add(envelope)
            await socket.send(json.dumps({"envelope_id": envelope}))
        elif kind == "acknowledged":
            envelope = frame.get("envelope_id")
            if envelope not in pending or not isinstance(
                frame.get("resume_cursor"), str
            ):
                raise SetupRequired("Ando acknowledged an unknown delivery")
            pending.remove(envelope)
            state.set("cursor", frame["resume_cursor"])
        elif kind == "disconnect":
            if isinstance(frame.get("resume_cursor"), str):
                state.set("cursor", frame["resume_cursor"])
            if frame.get("reason") == "policy_violation":
                raise SetupRequired("Ando realtime access was revoked")
            delay = frame.get("retry_after_seconds", 0)
            return min(max(float(delay), 0), 60)
        else:
            raise SetupRequired("Unsupported Ando realtime frame")
    return 0


async def listen(transport, state, on_event, on_connected, recover=None):
    try:
        ticket = await transport.ticket(state.get("cursor"))
    except ReplayExpired:
        if recover is None or transport.connection is None:
            # Legacy realtime-only receivers have no authoritative inbox sweep.
            raise
        # Invitation inbox claims/history remain the authority for pending work.
        # Subscribe first, then sweep; do not discard delivery/outbox state.
        ticket = await transport.ticket(None)
    # The ticket floor is an authoritative initial checkpoint, not an event cursor.
    state.set("cursor", ticket["resume_cursor"])
    async with websocket_connect(
        ticket["url"],
        subprotocols=[PROTOCOL],
        max_size=512 * 1024,
        open_timeout=30,
        close_timeout=10,
    ) as socket:
        if socket.subprotocol != PROTOCOL:
            raise SetupRequired("Ando realtime did not negotiate its protocol")
        on_connected()
        # Subscribe before sweeping: new arrivals are queued on this socket while
        # the inbox covers work from before the ticket floor. IDs deduplicate overlap.
        if recover:
            await recover()
        if recover is None:
            return await receive_socket(socket, state, on_event)

        async def reconcile():
            while True:
                await asyncio.sleep(30)
                await recover()

        reader = asyncio.create_task(receive_socket(socket, state, on_event))
        sweeper = asyncio.create_task(reconcile())
        try:
            done, _ = await asyncio.wait(
                [reader, sweeper], return_when=asyncio.FIRST_COMPLETED
            )
            if sweeper in done:
                await sweeper
            return await reader
        finally:
            import contextlib

            for task in (reader, sweeper):
                task.cancel()
            for task in (reader, sweeper):
                with contextlib.suppress(asyncio.CancelledError):
                    await task
