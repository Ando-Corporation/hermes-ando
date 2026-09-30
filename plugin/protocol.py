"""Provider-independent checks for the documented ando.realtime.v1 wire shape."""

from urllib.parse import urlsplit

PROTOCOL = "ando.realtime.v1"


class SetupRequired(RuntimeError):
    """A permanent failure: stop rather than silently pair or drop a replay gap."""


def resource_origin(resource_url):
    url = urlsplit(resource_url)
    local = url.hostname in {"localhost", "127.0.0.1", "::1"}
    if (
        url.scheme != "https" and not (url.scheme == "http" and local)
    ) or not url.netloc:
        raise SetupRequired(
            "Ando requires HTTPS (HTTP is allowed only for local development)"
        )
    if url.username or url.password or url.fragment or url.query:
        raise SetupRequired("Invalid Ando MCP resource URL")
    return f"{url.scheme}://{url.netloc}"


def verify_identity(result, workspace_id, membership_id=None, agent_name=None):
    identity = result.get("identity", {})
    if (
        identity.get("identity_type") != "agent"
        or identity.get("workspace_id") != workspace_id
    ):
        raise SetupRequired("Ando connection is not the requested workspace's agent")
    actual = identity.get("workspace_membership_id")
    if (
        not isinstance(actual, str)
        or not actual
        or (membership_id and actual != membership_id)
    ):
        raise SetupRequired("Ando connection belongs to a different agent")
    if agent_name is not None and identity.get("display_name") != agent_name:
        raise SetupRequired("Ando agent name does not match this creation request")
    return identity


def verify_ticket(ticket, origin, membership_id):
    target = urlsplit(ticket.get("url", ""))
    source = urlsplit(origin)
    scheme = "wss" if source.scheme == "https" else "ws"
    if (target.scheme, target.netloc) != (scheme, source.netloc):
        raise SetupRequired("Ando realtime ticket returned an unexpected server")
    if ticket.get("protocol") != PROTOCOL or not isinstance(
        ticket.get("resume_cursor"), str
    ):
        raise SetupRequired("Ando realtime ticket has an unsupported protocol")
    subscriptions = ticket.get("subscriptions")
    if not subscriptions or any(
        sub.get("target") != {"id": membership_id, "type": "workspace_membership"}
        for sub in subscriptions
    ):
        raise SetupRequired("Ando realtime subscription belongs to a different agent")


def verify_workspace_info(info, workspace_id, installer_id=None):
    if info.get("workspace", {}).get("workspace_id") != workspace_id:
        raise SetupRequired("Ando workspace orientation does not match this agent")
    installer = info.get("installed_by") or {}
    actual = installer.get("workspace_membership_id")
    if not actual or (installer_id is not None and actual != installer_id):
        raise SetupRequired(
            "Ando installer is missing or differs from the configured installer"
        )
    return actual


def message_reference(frame, workspace_id, membership_id):
    event = frame.get("payload", {})
    if event.get("workspace_id") != workspace_id:
        raise SetupRequired("Received an event from a different workspace")
    if event.get("type") != "message.created":
        return None
    obj = event.get("data", {}).get("object", {})
    required = [
        event.get("id"),
        obj.get("id"),
        obj.get("conversation_id"),
        obj.get("authorWorkspaceMembershipId"),
    ]
    if not all(isinstance(value, str) and value for value in required):
        raise SetupRequired("Malformed Ando message event")
    if obj["authorWorkspaceMembershipId"] == membership_id:
        return None
    return {
        "event_id": event["id"],
        "message_id": obj["id"],
        "conversation_id": obj["conversation_id"],
        "author_id": obj["authorWorkspaceMembershipId"],
        "thread_id": event.get("related", {}).get("thread_root_message_id"),
    }
