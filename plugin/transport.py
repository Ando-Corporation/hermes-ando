"""Native Hermes OAuth + MCP transport and ordered Ando realtime delivery.

The exact configured MCP URL remains the OAuth resource, including its pairing
scope. Only the ticket endpoint is derived from that resource's origin. OAuth
secrets stay in Hermes's own manager/storage; none are copied into plugin state.
"""

import asyncio
import contextlib
from contextlib import asynccontextmanager
import json
import logging

from .protocol import PROTOCOL, SetupRequired, resource_origin, verify_ticket

logger = logging.getLogger(__name__)

# What every connection names itself on MCP initialize; Ando compares it with
# the published release to tell the installer when to update (plugin.yaml).
CLIENT_INFO = {"name": "Ando Hermes", "version": "0.2.7"}

# Realtime wakes the agent for its DMs and mentions, and every new socket
# (at least every 15 minutes) sweeps the inbox, so this reconcile only catches
# work the socket does not deliver, such as replies in subscribed threads. At
# 30 seconds it was about half of Ando's production inbox reads (2026-10-17).
RECONCILE_INTERVAL_SECONDS = 300


def websocket_connect(*args, **kwargs):
    # Keep the optional socket dependency at the actual network boundary.
    from websockets.asyncio.client import connect
    return connect(*args, **kwargs)


class ToolError(RuntimeError):
    """Ando answered the tool call with an error: a definite refusal, not a lost connection."""


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
                        client_info=Implementation(**CLIENT_INFO),
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
                        client_info=Implementation(**CLIENT_INFO),
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
            raise ToolError(f"Ando {name} failed; review the connection in Ando")
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
            logger.info(
                "Ando realtime subscribed (connection %s, %d subscription(s))",
                frame.get("connection_id"),
                len(frame.get("subscriptions") or []),
            )
            continue
        if kind == "event":
            envelope = frame.get("envelope_id")
            if not isinstance(envelope, str) or not envelope:
                raise SetupRequired("Ando event has no delivery envelope")
            payload = frame.get("payload") or {}
            logger.info(
                "Ando realtime event %s (%s)", payload.get("id"), payload.get("type")
            )
            try:
                await on_event(frame)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Ando realtime event %s failed in the handler", payload.get("id")
                )
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
            logger.debug("Ando realtime acknowledged %s", envelope)
        elif kind == "disconnect":
            logger.info(
                "Ando realtime disconnect (%s, retry after %ss)",
                frame.get("reason"),
                frame.get("retry_after_seconds", 0),
            )
            if isinstance(frame.get("resume_cursor"), str):
                state.set("cursor", frame["resume_cursor"])
            if frame.get("reason") == "policy_violation":
                raise SetupRequired("Ando realtime access was revoked")
            delay = frame.get("retry_after_seconds", 0)
            return min(max(float(delay), 0), 60)
        else:
            raise SetupRequired(f"Unsupported Ando realtime frame type {kind!r}")
    logger.info("Ando realtime socket closed by the server")
    return 0


async def listen(transport, state, on_event, on_connected, wake=None):
    """One realtime connection. Inbox receivers pass wake, called once subscribed."""
    try:
        ticket = await transport.ticket(state.get("cursor"))
    except ReplayExpired:
        if wake is None or transport.connection is None:
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
        logger.info(
            "Ando realtime connected (connection %s, %s)",
            ticket.get("connection_id"),
            ", ".join(
                f"{sub.get('delivery')}: {'/'.join(sub.get('events') or [])}"
                for sub in ticket.get("subscriptions") or []
            ),
        )
        # Subscribe before sweeping: new arrivals are queued on this socket while
        # the inbox covers work from before the ticket floor. IDs deduplicate overlap.
        if wake:
            wake()
        return await receive_socket(socket, state, on_event)


async def receive_inbox(transport, state, inbox, on_connected):
    """Invitation receiving: realtime wakes one inbox worker that outlives sockets.

    Ando rotates sockets at least every 15 minutes and closes one whose event
    stays unacknowledged for a minute. A socket that drops mid-turn reconnects
    here while the worker finishes the turn and keeps sweeping, so a reconnect
    never abandons a reply or leaves the inbox unread. Returns only by raising.
    """

    async def reconcile():
        while True:
            await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)
            inbox.wake()

    worker = asyncio.create_task(inbox.run())
    sweeper = asyncio.create_task(reconcile())
    reader = None
    delay = 1
    try:
        while True:
            reader = asyncio.create_task(
                listen(transport, state, inbox.on_event, on_connected, inbox.wake)
            )
            await asyncio.wait([worker, reader], return_when=asyncio.FIRST_COMPLETED)
            if worker.done():
                await worker
                raise RuntimeError("Ando inbox worker stopped")
            try:
                retry_after = reader.result()
                delay = 1
            except SetupRequired:
                raise
            except Exception as exc:
                retry_after = delay
                delay = min(delay * 2, 60)
                logger.warning(
                    "Ando realtime dropped; reconnecting in %s seconds while the inbox worker continues (%s)",
                    retry_after,
                    type(exc).__name__,
                )
            await asyncio.wait([worker], timeout=retry_after)
    finally:
        tasks = [
            task
            for task in (reader, worker, sweeper, inbox.approvals_task)
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        for task in tasks:
            # A failed task's error already propagated above.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
