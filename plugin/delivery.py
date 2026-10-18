"""Ando event -> one Hermes turn -> durable, idempotent Ando reply."""

import contextlib
import logging

from . import files
from .protocol import SetupRequired, message_reference

logger = logging.getLogger(__name__)

# Posted once when a turn stopped before it prepared a reply. Its tools may
# have run, so the member decides whether to ask again.
INTERRUPTED_NOTICE = (
    "I stopped before I could finish replying to this. "
    "Send it again if you still need it, and check anything I may have started."
)


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
        # This turn's status messages: posted but never part of its reply.
        self.status_messages = set()
        self.status_keys = {}

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
            # Turning these off only stops files; the turn itself still runs.
            "upload_attachment",
            "get_file",
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
        if ref is None:
            logger.debug("Ando event is not another member's new message; no turn")
            return
        if self.state.completed(ref["event_id"]):
            logger.debug("Ando message %s was already answered", ref["message_id"])
            return
        if (
            self.allowed_users is not None
            and "*" not in self.allowed_users
            and ref["author_id"] not in self.allowed_users
        ):
            # The adapter's explicit local allowlist can be narrower than Ando's access.
            logger.warning(
                "Ando message %s is from member %s, who is not in allowed_users; no turn. "
                "Set ANDO_ALLOWED_USERS or ANDO_ALLOW_ALL_USERS to admit them",
                ref["message_id"],
                ref["author_id"],
            )
            self.state.complete(ref["event_id"])
            return
        if (
            self.allowed_conversations
            and ref["conversation_id"] not in self.allowed_conversations
        ):
            logger.warning(
                "Ando message %s is in conversation %s, which is not in allowed_conversations; no turn",
                ref["message_id"],
                ref["conversation_id"],
            )
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
        if self.active and self.active["event_id"] == ref["event_id"]:
            logger.debug("Ando message %s is already being answered", ref["message_id"])
            return
        if self.state.started(ref["event_id"]) and not (
            self.state.replies(ref["event_id"])
            or self.state.pending_uploads(ref["event_id"])
        ):
            # A turn began and stopped before freezing a reply: a crash, a
            # restart or a failed generation. Never regenerate it, because its
            # tools may already have run. Tell the member once through the
            # durable outbox below instead of stopping all receiving.
            tool, target = self._reply_target(ref)
            self.state.prepare_reply(
                ref["event_id"], 0, tool, {**target, "markdown_content": INTERRUPTED_NOTICE}
            )
            logger.warning(
                "Ando message %s: the Hermes turn stopped before replying; telling the member to ask again",
                ref["message_id"],
            )
        stored = self.state.replies(ref["event_id"])
        if stored or self.state.pending_uploads(ref["event_id"]):
            # A prior process prepared a response. Resume its exact write, never regenerate it.
            # Attachments frozen before a crash finish uploading under the same keys first.
            for ordinal, arguments in await files.resume_reply_files(
                self.transport,
                self.state,
                ref["event_id"],
                can_upload="upload_attachment" not in self.disabled_names,
            ):
                tool, target = self._reply_target(ref)
                self.state.prepare_reply(
                    ref["event_id"], ordinal, tool, {**target, **arguments}
                )
            for ordinal, tool, arguments, message_id in self.state.replies(
                ref["event_id"]
            ):
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
        self.active = ref
        self.ordinal = 0
        self.status_messages = set()
        self.status_keys = {}
        self.state.begin(ref["event_id"])
        logger.info(
            "Ando starting a Hermes turn for message %s in %s",
            ref["message_id"],
            ref["conversation_id"],
        )
        try:
            await self.dispatch(ref, message, self.disabled_names)
            replies = self.state.replies(ref["event_id"])
            if not replies or any(not reply[3] for reply in replies):
                raise RuntimeError("Hermes finished without a confirmed Ando reply")
            self.state.complete(ref["event_id"])
            logger.info("Ando replied to message %s", ref["message_id"])
        finally:
            self.active = None
            self.status_messages = set()
            self.status_keys = {}

    @staticmethod
    def _message_id(result):
        value = result.get("message_id")
        if not isinstance(value, str) or not value:
            raise RuntimeError("Ando did not confirm the sent message")
        return value

    @staticmethod
    def _reply_target(ref):
        if ref["thread_id"]:
            return "reply_to_message", {"message_id": ref["thread_id"]}
        return "send_message", {"conversation_id": ref["conversation_id"]}

    def _reply_call(self, chat_id, content, *, attachments=False):
        ref = self.active
        if ref is None or chat_id != self.chat_id(ref):
            raise SetupRequired(
                "Ando sends must reply to the active authorized conversation"
            )
        if not attachments and (not isinstance(content, str) or not content.strip()):
            raise ValueError("An Ando reply must contain text")
        tool, args = self._reply_target(ref)
        if tool in self.disabled_names:
            raise SetupRequired("Ando replies are disabled in Settings > Members")
        if isinstance(content, str) and content.strip():
            args["markdown_content"] = content
        return ref, tool, args

    async def send(self, chat_id, content):
        ref, tool, args = self._reply_call(chat_id, content)
        return await self._send_frozen(ref, tool, args)

    async def send_files(self, chat_id, paths, caption=None):
        """One durable reply carrying local files: frozen, uploaded once, posted once.

        paths is [(path, filename or None)]. Raises files.AttachmentFailed before
        anything is posted, so Hermes can report the failure in its own words.
        """
        if "upload_attachment" in self.disabled_names:
            raise files.AttachmentFailed("Ando file uploads are disabled in Settings > Members")
        ref, tool, args = self._reply_call(chat_id, caption, attachments=True)
        local = files.local_files(paths)
        reply = await files.upload_reply_files(
            self.transport, self.state, ref["event_id"], self.ordinal, local, caption
        )
        args.pop("markdown_content", None)
        return await self._send_frozen(ref, tool, {**args, **reply})

    async def fetch_attachments(self, message):
        return await files.fetch_attachments(
            self.transport, message, can_read="get_file" not in self.disabled_names
        )

    async def _send_frozen(self, ref, tool, args):
        args, message_id = self.state.prepare_reply(
            ref["event_id"], self.ordinal, tool, args
        )
        if message_id is None:
            result = await self.transport.call(tool, args)
            message_id = self._message_id(result)
            self.state.sent(ref["event_id"], self.ordinal, message_id)
        self.ordinal += 1
        return message_id

    async def send_status(self, chat_id, content, key=None):
        """A mid-turn status line, such as Hermes's "still working" heartbeat.

        It goes where the reply goes but never into the outbox: it is not the
        turn's reply, and a restart does not resend it. With a key, a later
        status of the same kind edits the first instead of adding another.
        """
        if key is not None and key in self.status_keys:
            message_id = self.status_keys[key]
            with contextlib.suppress(Exception):
                await self.edit_status(chat_id, message_id, content)
                return message_id
            self.status_keys.pop(key, None)
        _, tool, args = self._reply_call(chat_id, content)
        message_id = self._message_id(await self.transport.call(tool, args))
        self.status_messages.add(message_id)
        if key is not None:
            self.status_keys[key] = message_id
        return message_id

    def _own_status(self, chat_id, message_id):
        self._reply_call(chat_id, "status")
        if message_id not in self.status_messages:
            raise SetupRequired("Ando changes only this turn's own status messages")

    async def edit_status(self, chat_id, message_id, content):
        self._own_status(chat_id, message_id)
        if not isinstance(content, str) or not content.strip():
            raise ValueError("An Ando status must contain text")
        await self.transport.call(
            "edit_message", {"message_id": message_id, "markdown_content": content}
        )

    async def delete_status(self, chat_id, message_id):
        self._own_status(chat_id, message_id)
        await self.transport.call("delete_message", {"message_id": message_id})
        self.status_messages.discard(message_id)

    def chat_id(self, ref):
        # Hermes only namespaces Slack by scope_id, so namespace our chat ID explicitly.
        return f"{self.workspace_id}/{self.membership_id}/{ref['conversation_id']}"
