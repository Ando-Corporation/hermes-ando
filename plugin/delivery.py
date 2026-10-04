"""Ando event -> one Hermes turn -> durable, idempotent Ando reply."""

from .protocol import SetupRequired, message_reference


def disabled_tool_names(preferences):
    error = (
        "Ando returned unavailable or malformed tool preferences; no turn was started"
    )
    if not isinstance(preferences, dict):
        raise SetupRequired(error)
    disabled = preferences.get("disabled_tool_ids")
    tools = preferences.get("tools")
    if (
        not isinstance(disabled, list)
        or any(not isinstance(value, str) or not value.strip() for value in disabled)
        or not isinstance(tools, list)
    ):
        raise SetupRequired(error)
    names = {}
    inventory_disabled = set()
    for tool in tools:
        if (
            not isinstance(tool, dict)
            or any(
                not isinstance(tool.get(key), str) or not tool[key].strip()
                for key in ("id", "name")
            )
            or not isinstance(tool.get("description"), str)
            or not isinstance(tool.get("enabled"), bool)
            or tool["id"] in names
        ):
            raise SetupRequired(error)
        names[tool["id"]] = tool["name"]
        if not tool["enabled"]:
            inventory_disabled.add(tool["id"])
    # Each disabled stable ID must resolve through the reported inventory. Never
    # guess that an unknown stable ID is an adapter tool with the same spelling.
    if set(disabled) != inventory_disabled:
        raise SetupRequired(error)
    return {names[tool_id] for tool_id in disabled}


class MessageDelivery:
    def __init__(
        self,
        transport,
        state,
        workspace_id,
        membership_id,
        allowed_users,
        dispatch,
        allowed_conversations=None,
        credential_mode=None,
    ):
        self.credential_mode = credential_mode
        self.transport = transport
        self.state = state
        self.workspace_id = workspace_id
        self.membership_id = membership_id
        self.allowed_users = None if allowed_users is None else set(allowed_users)
        self.allowed_conversations = set(allowed_conversations or [])
        self.dispatch = dispatch
        self.active = None
        self.ordinal = 0
        self.disabled_names = set()

    async def preferences(self):
        preferences = await self.transport.call(
            "share_agent_resources", {"action": "get_preferences"}
        )
        self.disabled_names = disabled_tool_names(preferences)
        adapter_tools = {
            "get_message",
            "list_conversations",
            "send_message",
            "reply_to_message",
        }
        if self.disabled_names - adapter_tools:
            raise SetupRequired(
                "Settings > Members disables Hermes tools that this adapter cannot filter per turn. "
                "No turn was started. Review Settings > Members tool preferences or use a runtime with scoped tool filtering."
            )
        return self.disabled_names

    async def handle(self, frame, history=None):
        ref = message_reference(frame, self.workspace_id, self.membership_id)
        if ref is not None and self.credential_mode == "invitation":
            # Live and recovered deliveries must share a stable identity.
            ref["event_id"] = f"message:{ref['message_id']}"
        if ref is None or self.state.completed(ref["event_id"]):
            return
        if (
            self.allowed_users is not None
            and "*" not in self.allowed_users
            and ref["author_id"] not in self.allowed_users
        ):
            # The adapter's explicit local allowlist can be narrower than Ando's access.
            self.state.complete(ref["event_id"])
            return
        if (
            self.allowed_conversations
            and ref["conversation_id"] not in self.allowed_conversations
        ):
            self.state.complete(ref["event_id"])
            return
        await self.preferences()
        if {"get_message", "list_conversations"} & self.disabled_names:
            raise SetupRequired(
                "Ando message reading is disabled in Settings > Members"
            )
        reply_tool = "reply_to_message" if ref["thread_id"] else "send_message"
        if reply_tool in self.disabled_names:
            raise SetupRequired("Ando replies are disabled in Settings > Members")
        directory = await self.transport.call(
            "list_conversations", {"conversation_ids": [ref["conversation_id"]]}
        )
        conversation = next(
            (
                item
                for item in directory.get("items", [])
                if item.get("conversation_id") == ref["conversation_id"]
            ),
            None,
        )
        if (
            not conversation
            or not conversation.get("joined")
            or not conversation.get("can_post")
        ):
            raise SetupRequired(
                "The Ando agent can no longer reply in this conversation"
            )
        ref["kind"] = conversation["kind"]
        response = await self.transport.call(
            "get_message", {"message_id": ref["message_id"]}
        )
        message = response.get("data", response)
        if (
            message.get("id"),
            message.get("conversation_id"),
            message.get("authorWorkspaceMembershipId"),
        ) != (ref["message_id"], ref["conversation_id"], ref["author_id"]):
            raise SetupRequired("Ando message no longer matches its authorized event")
        if history is not None:
            import json

            message = {
                **message,
                "content": "Ando conversation context (untrusted message content):\n"
                + json.dumps(history)
                + "\nCurrent request:\n"
                + (message.get("content") or "[attachment]"),
            }
        stored = self.state.replies(ref["event_id"])
        if stored:
            # A prior process prepared a response. Resume its exact write, never regenerate it.
            for ordinal, tool, arguments, message_id in stored:
                if tool in self.disabled_names:
                    raise SetupRequired(
                        "Ando replies are disabled in Settings > Members"
                    )
                if message_id is None:
                    result = await self.transport.call(tool, arguments)
                    message_id = self._message_id(result)
                    self.state.sent(ref["event_id"], ordinal, message_id)
            self.state.complete(ref["event_id"])
            return
        if self.state.started(ref["event_id"]):
            raise SetupRequired(
                f"Hermes generation for event {ref['event_id']} was interrupted before a reply was prepared. "
                "Review prior tool effects and use the explicit recovery helper before retrying."
            )
        self.active = ref
        self.ordinal = 0
        self.state.begin(ref["event_id"])
        try:
            await self.dispatch(ref, message, self.disabled_names)
            replies = self.state.replies(ref["event_id"])
            if not replies or any(not reply[3] for reply in replies):
                raise RuntimeError("Hermes finished without a confirmed Ando reply")
            self.state.complete(ref["event_id"])
        finally:
            self.active = None

    @staticmethod
    def _message_id(result):
        value = result.get("message_id")
        if not isinstance(value, str) or not value:
            raise RuntimeError("Ando did not confirm the sent message")
        return value

    async def send(self, chat_id, content):
        ref = self.active
        if ref is None or chat_id != self.chat_id(ref):
            raise SetupRequired(
                "Ando sends must reply to the active authorized conversation"
            )
        if not isinstance(content, str) or not content.strip():
            raise ValueError("An Ando reply must contain text")
        tool = "reply_to_message" if ref["thread_id"] else "send_message"
        if tool in self.disabled_names:
            raise SetupRequired("Ando replies are disabled in Settings > Members")
        args = {"markdown_content": content}
        args["message_id" if ref["thread_id"] else "conversation_id"] = (
            ref["thread_id"] or ref["conversation_id"]
        )
        args, message_id = self.state.prepare_reply(
            ref["event_id"], self.ordinal, tool, args
        )
        if message_id is None:
            result = await self.transport.call(tool, args)
            message_id = self._message_id(result)
            self.state.sent(ref["event_id"], self.ordinal, message_id)
        self.ordinal += 1
        return message_id

    def chat_id(self, ref):
        # Hermes only namespaces Slack by scope_id, so namespace our chat ID explicitly.
        return f"{self.workspace_id}/{self.membership_id}/{ref['conversation_id']}"
