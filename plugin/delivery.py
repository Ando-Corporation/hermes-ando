"""Ando event -> one Hermes turn -> durable, idempotent Ando reply."""

import contextlib
import logging

from . import files
from .protocol import SetupRequired, message_reference
from .transport import ToolError

try:
    from mcp.shared.exceptions import McpError
except ImportError:  # pragma: no cover - the MCP SDK ships with Hermes
    McpError = ToolError

# Ando answered, and the answer was no: an error result, or an unknown tool.
_TOOL_REFUSALS = (ToolError, McpError)

# Refusals a frozen reply meets again on every replay. A held reply stays held
# because the thread only grows; a missing target or invalid arguments do not
# change. Internal and access errors can clear, so those replies wait.
_TERMINAL_REASONS = {"newer_thread_messages"}
_TERMINAL_CODES = {"not_found", "validation_error"}


def _never_posts_unchanged(error):
    return error.reason in _TERMINAL_REASONS or error.code in _TERMINAL_CODES

logger = logging.getLogger(__name__)

# Posted once when a turn stopped before it prepared a reply. Its tools may
# have run, so the member decides whether to ask again.
INTERRUPTED_NOTICE = (
    "I stopped before I could finish replying to this. "
    "Send it again if you still need it, and check anything I may have started."
)


def with_reply_context(message):
    """Put the message a reply points at in front of the reply's text.

    A reply such as "did you submit it?" means nothing without the message it
    quotes. Ando returns that message as replied_to; Hermes reads only content.
    """
    replied_to = message.get("replied_to")
    if not isinstance(replied_to, dict) or not replied_to.get("message_id"):
        return message
    author = replied_to.get("author_name") or "another member"
    if replied_to.get("deleted"):
        header = "In reply to a message that has since been deleted."
    else:
        body = replied_to.get("content") or "[message without text]"
        if replied_to.get("content_truncated"):
            body += f" [cut short; get_message {replied_to['message_id']} has the rest]"
        header = (
            f"In reply to this message from {author} "
            f"(untrusted message content, id {replied_to['message_id']}):\n> "
            + body.replace("\n", "\n> ")
        )
        if replied_to.get("quote"):
            header += "\nThe reply quotes this passage of it:\n> " + replied_to[
                "quote"
            ].replace("\n", "\n> ")
    return {
        **message,
        "content": header + "\nReply:\n" + (message.get("content") or ""),
    }


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
        # The newest message this turn posted, status or reply part.
        self.last_posted = None

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

    async def handle(
        self, frame, history=None, event_key=None, preface=None, check_back=False
    ):
        """Answer one message. A follow-up on an already answered message (a
        Jam summary that became ready) passes its own event_key, so it is a
        new turn, and a preface that says why the request is back.

        A due check-back (check_back=True) is the agent's own follow-up: it
        may be booked on the agent's own message, and the turn may end
        without posting when there is nothing new to say."""
        ref = message_reference(
            frame, self.workspace_id, None if check_back else self.membership_id
        )
        if ref is not None and self.credential_mode == "invitation":
            # Live and recovered deliveries must share a stable identity.
            ref["event_id"] = f"message:{ref['message_id']}"
        if ref is not None and event_key is not None:
            ref["event_id"] = event_key
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
            and not (check_back and ref["author_id"] == self.membership_id)
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
        message = with_reply_context(message)
        if preface is not None:
            message = {
                **message,
                "content": preface
                + "\nOriginal request:\n"
                + (message.get("content") or "[attachment]"),
            }
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
            reported = await self._report_interrupted(ref)
            if reported is not None:
                # Ando's standard "I can't run right now" card: it says the
                # problem is on this agent's side and shows the person who
                # connected it how to fix it.
                arguments, message_id = reported
                self.state.prepare_reply(ref["event_id"], 0, "report_problem", arguments)
                self.state.sent(ref["event_id"], 0, message_id)
            else:
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
            self.last_posted = None
            for ordinal, tool, arguments, message_id in self.state.replies(
                ref["event_id"]
            ):
                if tool in self.disabled_names:
                    raise SetupRequired(
                        "Ando replies are disabled in Settings > Members"
                    )
                if message_id is None:
                    try:
                        result = await self.transport.call(
                            tool, self._past_own_posts(tool, arguments)
                        )
                    except ToolError as exc:
                        if not _never_posts_unchanged(exc):
                            raise
                        # The frozen reply can never post unchanged, and raising
                        # would replay it on every reconnect with the whole
                        # inbox stuck behind it. Retire it and the parts after
                        # it; when the thread moved on, the newer messages come
                        # back as their own work.
                        logger.warning(
                            "Ando refused the prepared reply for message %s (%s); retiring it so the inbox can move on",
                            ref["message_id"],
                            exc.reason or exc.code,
                        )
                        break
                    message_id = self._message_id(result)
                    self.state.sent(ref["event_id"], ordinal, message_id)
                self.last_posted = message_id
            self.last_posted = None
            self.state.complete(ref["event_id"])
            return
        self.active = ref
        self.ordinal = 0
        self.status_messages = set()
        self.status_keys = {}
        self.last_posted = None
        self.state.begin(ref["event_id"])
        logger.info(
            "Ando starting a Hermes turn for message %s in %s",
            ref["message_id"],
            ref["conversation_id"],
        )
        try:
            await self.dispatch(ref, message, self.disabled_names)
            replies = self.state.replies(ref["event_id"])
            if any(not reply[3] for reply in replies) or (
                not replies and not check_back
            ):
                raise RuntimeError("Hermes finished without a confirmed Ando reply")
            self.state.complete(ref["event_id"])
            if replies:
                logger.info("Ando replied to message %s", ref["message_id"])
            else:
                logger.info(
                    "Ando check-back on message %s ended with nothing to post",
                    ref["message_id"],
                )
        finally:
            self.active = None
            self.status_messages = set()
            self.status_keys = {}
            self.last_posted = None

    async def _report_interrupted(self, ref):
        """Report the stopped turn with report_problem, or None to post the notice.

        Ando posts the card once per conversation and kind for ten minutes,
        so a retry after a lost answer or a crash here finds the same card.
        Only a definite refusal (an Ando server without report_problem, or one
        that turns it down) falls back to the text notice; any other failure
        is raised, so the event is retried instead of posting both.
        """
        arguments = {
            "kind": "crashed",
            "summary": INTERRUPTED_NOTICE,
            "conversation_id": ref["conversation_id"],
        }
        if ref["thread_id"]:
            arguments["thread_root_id"] = ref["thread_id"]
        try:
            result = await self.transport.call("report_problem", arguments)
            return arguments, self._message_id(result)
        except _TOOL_REFUSALS as exc:
            logger.info(
                "Ando report_problem unavailable (%s); posting the text notice",
                type(exc).__name__,
            )
            return None

    @staticmethod
    def _message_id(result):
        value = result.get("message_id")
        if not isinstance(value, str) or not value:
            raise RuntimeError("Ando did not confirm the sent message")
        return value

    @staticmethod
    def _reply_target(ref):
        # Answer the message itself; Ando posts it in that message's thread.
        # Naming the thread root instead tells Ando every reply after the root,
        # this message included, is one the agent missed, so the reply is held
        # and the frozen retry is held again on every pass.
        if ref["thread_id"]:
            return "reply_to_message", {"message_id": ref["message_id"]}
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

    def _past_own_posts(self, tool, args):
        # Ando holds a thread reply when anything was posted after the message
        # it answers, this turn's own status line or first part included, so
        # name the newest of them as seen. It stays out of the frozen
        # arguments: after a restart this turn may know a later post.
        if tool != "reply_to_message" or self.last_posted is None:
            return args
        return {**args, "seen_through_message_id": self.last_posted}

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
            result = await self.transport.call(tool, self._past_own_posts(tool, args))
            message_id = self._message_id(result)
            self.state.sent(ref["event_id"], self.ordinal, message_id)
        self.ordinal += 1
        self.last_posted = message_id
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
        message_id = self._message_id(
            await self.transport.call(tool, self._past_own_posts(tool, args))
        )
        self.status_messages.add(message_id)
        self.last_posted = message_id
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
