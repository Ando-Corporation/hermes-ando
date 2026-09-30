"""Install/configure the Ando plugin from an Ando identity-scoped setup file.

Run with the Python interpreter belonging to the intended Hermes profile.
This command deliberately asks Hermes to own OAuth; it never handles passwords,
reads browser cookies, or chooses an identity on the user's behalf.
"""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from uuid import UUID

from plugin.protocol import SetupRequired, resource_origin, verify_identity
from plugin.transport import AndoTransport


def read_setup(path):
    value = json.loads(path.read_text())
    if value.get("schemaVersion") != 1 or value.get("harness") != "hermes":
        raise SetupRequired("This is not a supported Hermes setup file")
    if value.get("mode") not in {"create", "connect"}:
        raise SetupRequired("The setup file has an unsupported creation mode")
    UUID(value["workspaceId"])
    resource_origin(value["mcpUrl"])
    if (
        not isinstance(value.get("displayName"), str)
        or not value["displayName"].strip()
    ):
        raise SetupRequired("The setup file must name the intended agent")
    if value["mode"] == "connect":
        UUID(value["workspaceMembershipId"])
    return value


def prepare_server(config, setup):
    # A new pairing issuance gets a new entry; never redirect an existing grant.
    servers = config.setdefault("mcp_servers", {})
    digest = hashlib.sha256(setup["mcpUrl"].encode()).hexdigest()[:16]
    name = f"ando-gateway-{digest}"
    if name in servers:
        entry = servers[name]
        if (
            entry.get("url") != setup["mcpUrl"]
            or entry.get("auth") != "oauth"
            or entry.get("enabled") is not False
        ):
            raise SetupRequired(
                "Hermes MCP server name is already used by a different resource or runtime"
            )
        return name
    # This is a native gateway credential, not a second model tool connection.
    # Hermes's MCP worker owns another asyncio loop; its OAuth locks must not be
    # shared concurrently with this platform's loop. Login works while disabled.
    servers[name] = {"url": setup["mcpUrl"], "auth": "oauth", "enabled": False}
    return name


async def find_installer_dm(transport, installer_id, membership_id):
    cursor = None
    seen = set()
    while True:
        args = {"kind": "direct_message", "joined": True, "limit": 100}
        if cursor:
            args["cursor"] = cursor
        page = await transport.call("list_conversations", args)
        for conversation in page.get("items", []):
            conversation_id = conversation.get("conversation_id")
            if not conversation_id or conversation.get("archived"):
                continue
            result = await transport.call(
                "list_conversation_members", {"conversation_id": conversation_id}
            )
            members = result.get("members", result.get("items", []))
            member_ids = {member.get("workspaceMembershipId") for member in members}
            if member_ids == {installer_id, membership_id}:
                return conversation_id
        if not page.get("page", {}).get("has_more"):
            raise SetupRequired(
                "Ando has not created the installer DM yet. Retry setup after it appears."
            )
        cursor = page["page"].get("next_cursor")
        if not cursor or cursor in seen:
            raise SetupRequired("Ando conversation pagination did not advance")
        seen.add(cursor)


async def verify_setup(setup, server_name):
    transport = AndoTransport(server_name, setup.get("workspaceMembershipId"))
    async with transport.connected():
        result = await transport.call("get_current_identity", {})
        identity = verify_identity(
            result,
            setup["workspaceId"],
            setup.get("workspaceMembershipId"),
            setup["displayName"] if setup["mode"] == "create" else None,
        )
        UUID(identity["workspace_membership_id"])
        info = await transport.call("get_workspace_info", {})
        if info.get("workspace", {}).get("workspace_id") != setup["workspaceId"]:
            raise SetupRequired("Ando workspace does not match the setup file")
        installer = info.get("installed_by")
        if not installer or not installer.get("workspace_membership_id"):
            raise SetupRequired(
                "Ando could not resolve the installer; no runtime was configured"
            )
        installer_id = installer["workspace_membership_id"]
        conversation_id = await find_installer_dm(
            transport, installer_id, identity["workspace_membership_id"]
        )
        return identity, installer_id, conversation_id


def runtime_config(config, setup, server_name, identity, installer_id, conversation_id):
    platforms = config.setdefault("gateway", {}).setdefault("platforms", {})
    existing = platforms.get("ando")
    member_id = identity["workspace_membership_id"]
    if existing and existing.get("extra", {}).get("membership_id") != member_id:
        raise SetupRequired(
            "This Hermes profile already serves another Ando agent; use a separate Hermes profile"
        )
    platforms["ando"] = {
        "enabled": True,
        "gateway_restart_notification": False,
        "typing_indicator": False,
        "extra": {
            "mcp_server": server_name,
            "workspace_id": setup["workspaceId"],
            "membership_id": member_id,
            "installer_id": installer_id,
            "allowed_users": [installer_id],
            "allowed_conversations": [conversation_id],
            "group_sessions_per_user": False,
        },
    }


async def configure(path, skip_login=False):
    from hermes_cli.config import load_config, save_config
    from hermes_constants import get_hermes_home
    from tools.mcp_oauth import HermesTokenStorage

    setup = read_setup(path)
    config = load_config()
    existing = config.get("gateway", {}).get("platforms", {}).get("ando")
    if existing:
        extra = existing.get("extra", {})
        existing_server = config.get("mcp_servers", {}).get(extra.get("mcp_server"), {})
        same_attempt = (
            setup["mode"] == "create" and existing_server.get("url") == setup["mcpUrl"]
        )
        same_member = setup["mode"] == "connect" and extra.get(
            "membership_id"
        ) == setup.get("workspaceMembershipId")
        if extra.get("workspace_id") != setup["workspaceId"] or not (
            same_attempt or same_member
        ):
            raise SetupRequired(
                "Use a separate Hermes profile to create or connect another Ando agent"
            )
    server_name = prepare_server(config, setup)
    save_config(config)
    # `hermes mcp login` deliberately clears prior credentials. Never run it
    # again during an ordinary setup retry; refresh/verification owns reuse.
    if not skip_login and not HermesTokenStorage(server_name).has_cached_tokens():
        print(
            "Approve this Ando connection in the browser. Return here when approval finishes."
        )
        subprocess.run(
            [
                sys.executable,
                "-c",
                "from hermes_cli.main import main; main()",
                "mcp",
                "login",
                server_name,
            ],
            check=True,
        )
    identity, installer_id, conversation_id = await verify_setup(setup, server_name)
    # Re-read after OAuth; its CLI may have updated client metadata/config.
    config = load_config()
    runtime_config(config, setup, server_name, identity, installer_id, conversation_id)
    plugin_dir = Path(get_hermes_home()) / "plugins" / "ando-platform"
    source = Path(__file__).parent / "plugin"
    if plugin_dir.exists():
        for file in source.iterdir():
            if not file.is_file() or file.suffix == ".pyc":
                continue
            target = plugin_dir / file.name
            if not target.is_file() or target.read_bytes() != file.read_bytes():
                raise SetupRequired(
                    "An existing Ando plugin differs; review it before replacing it"
                )
    shutil.copytree(
        source,
        plugin_dir,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    plugins = config.setdefault("plugins", {})
    if "ando-platform" in plugins.get("disabled", []):
        raise SetupRequired(
            "The Ando plugin was explicitly disabled; enable it in Hermes before retrying"
        )
    plugins["enabled"] = sorted(set([*plugins.get("enabled", []), "ando-platform"]))
    save_config(config)
    print(
        f"Ando access verified for {identity.get('display_name')}. Runtime configured; replies are not verified yet."
    )
    print(
        "Run `hermes gateway run` in this profile, then send a new message in your existing Ando DM."
    )
    print(f"DM conversation: {conversation_id}")
    print(
        "First startup listens from now: resend any request sent before the gateway started."
    )
    print(
        "Keep the gateway's host running. The browser may close after OAuth approval."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Connect a Hermes gateway using an Ando setup download"
    )
    parser.add_argument("setup_file", type=Path)
    parser.add_argument(
        "--skip-login", action="store_true", help="Use an already approved OAuth grant"
    )
    args = parser.parse_args()
    try:
        asyncio.run(configure(args.setup_file, args.skip_login))
    except Exception as exc:

        def reason(error):
            if isinstance(error, SetupRequired):
                return str(error)
            for child in getattr(error, "exceptions", []):
                if found := reason(child):
                    return found
            return None

        message = (
            reason(exc)
            or f"{type(exc).__name__}; check the setup file and Hermes connection, then retry"
        )
        print(f"Ando setup stopped: {message}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
