"""Installed `hermes ando` commands. Secrets enter through stdin, never argv."""

import argparse
import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
import time
import uuid

from .connection import (
    private_write,
    read_connection,
    redeem_invitation,
    validate_connection,
)
from .protocol import (
    SetupRequired,
    lock_acquired,
    message_reference,
    release_scoped_lock,
    verify_identity,
    verify_workspace_info,
)
from .transport import AndoTransport


def profile_home():
    from hermes_constants import get_hermes_home

    return Path(get_hermes_home())


@contextmanager
def profile_lock(home):
    import fcntl

    directory = home / "ando"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (directory / "setup.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SetupRequired(
                "Another Ando setup is running in this Hermes profile"
            ) from None
        yield


def setup_parser(parser):
    sub = parser.add_subparsers(dest="operation", required=True)
    connect = sub.add_parser(
        "connect",
        help="Use the existing invitation or privately saved redemption response",
    )
    source = connect.add_mutually_exclusive_group()
    source.add_argument(
        "--invite-stdin",
        action="store_true",
        help="Read the existing invitation URL from stdin",
    )
    source.add_argument(
        "--credential-stdin",
        action="store_true",
        help="Read the saved redemption JSON from stdin",
    )
    connect.add_argument(
        "--name",
        help="This bot's existing display name; only used for a new invitation",
    )
    sub.add_parser(
        "status", help="Show non-secret identity and receiving configuration"
    )
    sub.add_parser(
        "doctor",
        help="Verify runtime dependencies, credentials and current Ando access",
    )
    test = sub.add_parser(
        "test",
        help="Check what would stop this profile receiving Ando messages",
    )
    test.add_argument(
        "--wait",
        type=int,
        default=0,
        metavar="SECONDS",
        help="With the gateway stopped, wait for a message sent to this agent in Ando",
    )
    sub.add_parser(
        "disconnect", help="Disable receiving; retain identity and recovery state"
    )
    recover = sub.add_parser(
        "recover",
        help="Allow an interrupted generation to retry after reviewing its effects",
    )
    recover.add_argument("event_id")
    recover.add_argument(
        "--previous-effects-reviewed", action="store_true", required=True
    )


def preflight(config):
    from .adapter import check_requirements

    if not check_requirements():
        raise SetupRequired(
            "Install the dependencies declared by the Ando plugin with Hermes's plugin manager"
        )
    if "ando-platform" in config.get("plugins", {}).get("disabled", []):
        raise SetupRequired(
            "The Ando plugin is disabled; enable it using Hermes's existing plugin controls"
        )


def configured_identity(config):
    return (
        config.get("gateway", {}).get("platforms", {}).get("ando", {}).get("extra", {})
    )


async def connect(args, home, config, save_config):
    import httpx

    preflight(config)
    path = home / "ando" / "connection.json"
    previous = read_connection(path) if path.exists() else None
    pending = path.with_name("connection-pending.json")
    importing = False
    if args.invite_stdin:
        if configured_identity(config) and previous is None:
            raise SetupRequired(
                "This profile has an existing OAuth receiver. Stop it and use that agent's reconnect response; never redeem a new-agent invitation"
            )
        async with httpx.AsyncClient(follow_redirects=False, timeout=30) as client:
            connection = await redeem_invitation(
                sys.stdin.read(4097).strip(), args.name, path, client
            )
    elif args.credential_stdin or pending.exists():
        importing = True
        connection = (
            json.loads(sys.stdin.read(65537))
            if args.credential_stdin
            else read_connection(pending)
        )
        validate_connection(connection)
        if previous and any(
            previous[k] != connection[k]
            for k in ("workspace_id", "agent_membership_id")
        ):
            raise SetupRequired(
                "Reconnect this profile's existing agent; do not replace its identity"
            )
        extra = configured_identity(config)
        if extra and (extra.get("workspace_id"), extra.get("membership_id")) != (
            connection["workspace_id"],
            connection["agent_membership_id"],
        ):
            raise SetupRequired("This Hermes profile already serves another Ando agent")
        connection["receiver_id"] = (
            previous.get("receiver_id", str(uuid.uuid4()))
            if previous
            else str(uuid.uuid4())
        )
        if args.credential_stdin and pending.exists():
            private_write(
                path.with_name(f"connection-recovery-{uuid.uuid4()}.json"),
                read_connection(pending),
            )
        private_write(pending if previous else path, connection)
    elif previous:
        connection = previous
    else:
        raise SetupRequired(
            "Give this bot the existing Ando invitation; no saved connection exists"
        )
    transport = AndoTransport(
        None, connection["agent_membership_id"], connection=connection
    )
    async with transport.connected():
        identity = await transport.call("get_current_identity", {})
        verify_identity(
            identity, connection["workspace_id"], connection["agent_membership_id"]
        )
        workspace = await transport.call("get_workspace_info", {})
        verify_workspace_info(
            workspace,
            connection["workspace_id"],
            connection["connected_by_membership_id"],
        )
    if importing:
        private_write(path, connection)
    pending.unlink(missing_ok=True)
    # First-time credentials remain private on disk if remote verification fails.
    # Retrying without input resumes it; never consume another invitation.
    extra = configured_identity(config)
    if extra and (extra.get("workspace_id"), extra.get("membership_id")) != (
        connection["workspace_id"],
        connection["agent_membership_id"],
    ):
        raise SetupRequired("This Hermes profile already serves another Ando agent")
    config.setdefault("gateway", {}).setdefault("platforms", {})["ando"] = {
        "enabled": True,
        "typing_indicator": False,
        "gateway_restart_notification": False,
        "extra": {
            "credential_mode": "invitation",
            "workspace_id": connection["workspace_id"],
            "membership_id": connection["agent_membership_id"],
            "installer_id": connection["connected_by_membership_id"],
            "allowed_users": (extra.get("allowed_users", [connection["connected_by_membership_id"]])
                              if extra else [connection["connected_by_membership_id"]]),
            "allowed_conversations": extra.get("allowed_conversations", []) if extra else [],
            "group_sessions_per_user": False,
        },
    }
    save_config(config)
    print(
        "Ando identity verified. Start or restart the existing Hermes gateway. Receiving is not yet verified."
    )
    print(
        "Complete the invitation's introduction once, then verify a fresh DM and follow-up while Hermes is idle."
    )
    # Let the bot follow the same server-issued orientation/introduction without
    # ever echoing the redemption credential or invitation code.
    print(
        json.dumps(
            {
                key: connection[key]
                for key in ("display_name", "conversations", "next_steps")
                if key in connection
            }
        )
    )


HELLO_TIMEOUT_SECONDS = 15


async def receiving_test(args, config, connection, *, socket_connect=None):
    """Read-only: claims nothing, acknowledges nothing, saves no cursor."""
    from gateway.status import acquire_scoped_lock
    from utils import is_truthy_value

    from .inbox import SUPPORTED_RECOVERY_TOOLS
    from .transport import websocket_connect
    from .protocol import PROTOCOL

    failures = []

    def report(ok, text):
        print(f"{'✓' if ok else '✗'} {text}")
        if not ok:
            failures.append(text)

    try:
        preflight(config)
        report(True, "Plugin enabled and its dependencies installed")
    except SetupRequired as exc:
        report(False, str(exc))
    report(
        config.get("gateway", {}).get("platforms", {}).get("ando", {}).get("enabled")
        is True,
        "Ando receiving enabled in this profile's config.yaml",
    )
    if os.environ.get("GATEWAY_RELAY_URL", "").strip() and not is_truthy_value(
        os.environ.get("GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS")
    ):
        report(
            False,
            "GATEWAY_RELAY_URL is set, so the gateway turns Ando off. Add "
            "GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS=true to this profile's .env",
        )
    transport = AndoTransport(
        None, connection["agent_membership_id"], connection=connection
    )
    async with transport.connected():
        verify_identity(
            await transport.call("get_current_identity", {}),
            connection["workspace_id"],
            connection["agent_membership_id"],
        )
        report(True, "Ando accepts this agent's credential")
        workspace = await transport.call("get_workspace_info", {})
        receiving = workspace.get("delivery", {}).get("receiving") or {}
        extra = configured_identity(config)
        report(
            not (
                extra.get("credential_mode") == "invitation"
                and receiving.get("configured_method") == "https"
            ),
            "Ando sends this agent's messages over WebSocket, not to another HTTPS receiver",
        )
        waiting = unsupported = 0
        for scope in ("direct", "updates"):
            args_ = {"scope": scope, "limit": 100}
            seen = set()
            while True:
                result = await transport.call("get_agent_inbox", args_)
                for item in result["items"]:
                    if item.get("type") in {"mention", "direct_message", "reply"}:
                        waiting += 1
                        tool = (item.get("recovery") or {}).get("tool")
                        unsupported += bool(tool and tool not in SUPPORTED_RECOVERY_TOOLS)
                page = result.get("page") or {}
                if not page.get("has_more"):
                    break
                cursor = page.get("next_cursor")
                if not cursor or cursor in seen:
                    raise SetupRequired("Ando inbox pagination did not advance")
                seen.add(cursor)
                args_ = {**args_, "cursor": cursor}
        report(
            not unsupported,
            f"Inbox readable: {waiting} message item(s) waiting"
            + (
                f", {unsupported} this plugin can't answer; upgrade it"
                if unsupported
                else ""
            ),
        )
        key = f"{connection['workspace_id']}:{connection['agent_membership_id']}"
        if not lock_acquired(acquire_scoped_lock("ando", key)):
            print(
                "  The gateway is running this receiver; its log shows each Realtime "
                "event. Stop the gateway to test Realtime here."
            )
            report(not args.wait, "--wait needs the gateway stopped")
        else:
            try:
                ticket = await transport.ticket(None)
                async with (socket_connect or websocket_connect)(
                    ticket["url"],
                    subprotocols=[PROTOCOL],
                    max_size=512 * 1024,
                    open_timeout=30,
                    close_timeout=10,
                ) as socket:
                    await realtime_check(socket, connection, args.wait, report)
            finally:
                release_scoped_lock("ando", key)
    if failures:
        raise SystemExit(1)


async def realtime_check(socket, connection, wait, report):
    async def frame(timeout):
        return json.loads(await asyncio.wait_for(socket.recv(), timeout))

    try:
        hello = await frame(HELLO_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        hello = {}
    if hello.get("type") != "hello":
        report(False, "Realtime connected but Ando never confirmed the subscription")
        return
    report(True, "Realtime connected and subscribed to new messages")
    if not wait:
        return
    print(
        f"  Send this agent a message in Ando now; waiting {wait}s. "
        "It stays unread for the gateway to answer."
    )
    deadline = time.monotonic() + wait
    while (left := deadline - time.monotonic()) > 0:
        try:
            event = await frame(left)
        except asyncio.TimeoutError:
            break
        if event.get("type") == "disconnect":
            report(False, f"Ando closed Realtime ({event.get('reason')})")
            return
        reference = event.get("type") == "event" and message_reference(
            event, connection["workspace_id"], connection["agent_membership_id"]
        )
        if reference:
            # No ACK: the gateway's inbox sweep still owns this message.
            report(True, f"Message {reference['message_id']} arrived over Realtime")
            return
    report(False, f"No message arrived over Realtime in {wait}s")


async def execute(args):
    from hermes_cli.config import load_config, save_config

    home = profile_home()
    with profile_lock(home):
        config = load_config()
        extra = configured_identity(config)
        if args.operation == "connect":
            from gateway.status import acquire_scoped_lock

            lock = f"{extra.get('workspace_id')}:{extra.get('membership_id')}"
            if not lock_acquired(acquire_scoped_lock("ando", lock)):
                raise SetupRequired(
                    "Stop the Ando receiver before changing its credentials"
                )
            try:
                await connect(args, home, config, save_config)
            finally:
                release_scoped_lock("ando", lock)
        elif args.operation == "disconnect":
            if extra:
                config["gateway"]["platforms"]["ando"]["enabled"] = False
                save_config(config)
            print(
                "Ando receiving disabled in configuration. Restart the gateway to apply. Identity and recovery state retained."
            )
        elif args.operation == "recover":
            from gateway.status import acquire_scoped_lock
            from .state import DeliveryState

            if not extra:
                raise SetupRequired("No configured Ando identity")
            lock = f"{extra['workspace_id']}:{extra['membership_id']}"
            if not lock_acquired(acquire_scoped_lock("ando", lock)):
                raise SetupRequired(
                    "Stop the gateway before recovering an interrupted generation"
                )
            try:
                state = DeliveryState(
                    home / "ando" / f"{extra['membership_id']}.sqlite3",
                    extra["workspace_id"],
                    extra["membership_id"],
                )
                try:
                    state.retry_after_review(args.event_id)
                finally:
                    state.close()
            finally:
                release_scoped_lock("ando", lock)
            print("Interrupted generation can be retried. Restart the gateway.")
        elif args.operation == "test":
            if not extra:
                raise SetupRequired("No configured Ando identity")
            connection = read_connection(home / "ando" / "connection.json")
            if (extra.get("workspace_id"), extra.get("membership_id")) != (
                connection["workspace_id"],
                connection["agent_membership_id"],
            ):
                raise SetupRequired(
                    "config.yaml and the saved credential name different Ando agents; run `hermes ando connect` again"
                )
            await receiving_test(args, config, connection)
        else:
            connection = read_connection(home / "ando" / "connection.json")
            if args.operation == "doctor":
                preflight(config)
                transport = AndoTransport(
                    None, connection["agent_membership_id"], connection=connection
                )
                async with transport.connected():
                    verify_identity(
                        await transport.call("get_current_identity", {}),
                        connection["workspace_id"],
                        connection["agent_membership_id"],
                    )
                print(
                    "Dependencies and current agent identity verified. Run `hermes ando test` to check receiving."
                )
            print(
                json.dumps(
                    {
                        "workspace_id": connection["workspace_id"],
                        "agent_membership_id": connection["agent_membership_id"],
                        "receiving_enabled": config.get("gateway", {})
                        .get("platforms", {})
                        .get("ando", {})
                        .get("enabled", False),
                    }
                )
            )


def run(args):
    try:
        asyncio.run(execute(args))
    except SetupRequired as exc:
        print(f"Ando setup stopped: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print(
            "Ando setup could not finish. Credentials, if issued, were retained; retry without redeeming another invitation. Use the existing reconnect flow if the redemption response was lost.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    setup_parser(parser)
    run(parser.parse_args())
