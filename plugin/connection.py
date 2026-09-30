"""Invite redemption and private, resumable profile credentials.

Only official invitation origins are accepted. Network errors never include
response bodies, URLs containing invite codes, or credential values.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import uuid
from urllib.parse import urlsplit

from .protocol import SetupRequired

ENVIRONMENTS = {
    "agents.ando.so": ("https://mcp.ando.so/mcp", "https://api.ando.so"),
    "staging-agents.ando.so": (
        "https://ando-mcp-staging.onrender.com/mcp",
        "https://ando-backend-staging.onrender.com",
    ),
}


def private_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def read_connection(path):
    path = Path(path)
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise SetupRequired(
            "Ando credentials must be a private regular file (mode 600)"
        )
    value = json.loads(path.read_text())
    validate_connection(value)
    return value


def validate_connection(value):
    if not isinstance(value, dict) or any(
        not isinstance(value.get(key), str) or not value[key].strip()
        for key in (
            "api_key",
            "workspace_id",
            "agent_membership_id",
            "connected_by_membership_id",
            "mcp_url",
        )
    ):
        raise SetupRequired(
            "Expected the complete private Ando invitation redemption response"
        )
    if value["mcp_url"] not in {endpoints[0] for endpoints in ENVIRONMENTS.values()}:
        raise SetupRequired("The invitation returned an unsupported MCP destination")
    if value.get("contract_version") != 1:
        raise SetupRequired("Unsupported Ando invitation contract")


def api_origin(connection):
    return next(
        api for mcp, api in ENVIRONMENTS.values() if mcp == connection["mcp_url"]
    )


def realtime_endpoint(connection):
    origin = api_origin(connection)
    prefix = "v1" if origin == "https://api.ando.so" else "api/v1"
    return f"{origin}/{prefix}/realtime/connections"


def websocket_origin(connection):
    return (
        "https://realtime.ando.so"
        if api_origin(connection) == "https://api.ando.so"
        else api_origin(connection)
    )


def invitation_url(value):
    url = urlsplit(value.strip())
    if (
        url.scheme != "https"
        or url.netloc not in ENVIRONMENTS
        or url.query
        or url.fragment
        or not re.fullmatch(r"/invite/(?:[a-hj-km-np-z2-9]{10}|[a-f0-9]{32})", url.path)
    ):
        raise SetupRequired(
            "Use the existing Ando agent invitation link from Invite members"
        )
    return url


async def redeem_invitation(url_text, name, path, client):
    url = invitation_url(url_text)
    digest = hashlib.sha256(url_text.strip().encode()).hexdigest()
    path = Path(path)
    if path.exists():
        existing = read_connection(path)
        if existing.get("invitation_digest") == digest:
            return existing
        # Reconnection may rotate the key, but may never replace this profile's identity.
    else:
        existing = None
    response = await client.get(url_text.strip())
    if response.status_code != 200:
        raise SetupRequired(
            "Invitation expired or was used. Reconnect the existing agent in Settings > Members"
        )
    text = response.text
    target = re.search(r"\(agent ([a-f0-9-]{36})\); keep this identity", text)
    if existing and (target is None or target[1] != existing["agent_membership_id"]):
        raise SetupRequired(
            "This profile already serves an agent; use its existing reconnect invitation"
        )
    if target is None and (not name or not name.strip() or len(name) > 80):
        raise SetupRequired(
            "Supply this bot's existing display name for a new invitation"
        )
    body = {"passcode": url.path.rsplit("/", 1)[1], "harness": "hermes"}
    if target is None:
        body["name"] = name.strip()
    # A failed/ambiguous POST must never be automatically retried: the code is one-use.
    response = await client.post(f"https://{url.netloc}/connect", json=body)
    if response.status_code != 200:
        raise SetupRequired(
            "Invitation redemption failed. Use Settings > Members to reconnect the existing agent"
        )
    value = response.json()
    # Retain a successful one-time response even if a newer server contract
    # prevents this version from configuring it. Never print that response.
    private_write(path.with_name("redemption-pending.json"), value)
    validate_connection(value)
    if value["mcp_url"] != ENVIRONMENTS[url.netloc][0]:
        raise SetupRequired("Invitation environment mismatch")
    if target and value["agent_membership_id"] != target[1]:
        raise SetupRequired("Invitation identity mismatch")
    if existing and (value["workspace_id"], value["agent_membership_id"]) != (
        existing["workspace_id"],
        existing["agent_membership_id"],
    ):
        raise SetupRequired("Invitation would replace the configured agent")
    value["receiver_id"] = (
        existing.get("receiver_id", str(uuid.uuid4()))
        if existing
        else str(uuid.uuid4())
    )
    value["invitation_digest"] = digest
    private_write(path, value)
    path.with_name("redemption-pending.json").unlink(missing_ok=True)
    return value
