"""Installed `hermes ando` commands. Secrets enter through stdin, never argv."""

import argparse
import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import uuid

from .connection import (
    private_write,
    read_connection,
    redeem_invitation,
    validate_connection,
)
from .protocol import SetupRequired, verify_identity, verify_workspace_info
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


async def execute(args):
    from hermes_cli.config import load_config, save_config

    home = profile_home()
    with profile_lock(home):
        config = load_config()
        extra = configured_identity(config)
        if args.operation == "connect":
            from gateway.status import acquire_scoped_lock, release_scoped_lock

            lock = f"{extra.get('workspace_id')}:{extra.get('membership_id')}"
            if not acquire_scoped_lock("ando", lock):
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
            from gateway.status import acquire_scoped_lock, release_scoped_lock
            from .state import DeliveryState

            if not extra:
                raise SetupRequired("No configured Ando identity")
            lock = f"{extra['workspace_id']}:{extra['membership_id']}"
            if not acquire_scoped_lock("ando", lock):
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
                    "Dependencies and current agent identity verified; a real reply is still required to verify receiving."
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
