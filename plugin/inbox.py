"""Use Ando's existing inbox for startup recovery and event-driven work.

Realtime is a wake signal. The inbox remains the authority for which messages
need attention, and its optional execution claim prevents competing receivers.
"""

import asyncio
import json

from .protocol import SetupRequired, message_reference


class InboxRecovery:
    def __init__(self, transport, delivery):
        self.transport = transport
        self.delivery = delivery
        self.lock = asyncio.Lock()
        self.owner = transport.connection["receiver_id"]

    async def on_event(self, frame):
        message_reference(
            frame, self.delivery.workspace_id, self.delivery.membership_id
        )
        await self.sweep()

    async def sweep(self):
        async with self.lock:
            for scope in ("direct", "updates"):
                args = {"scope": scope, "limit": 100}
                seen = set()
                while True:
                    result = await self.transport.call("get_agent_inbox", args)
                    for item in result["items"]:
                        if item.get("type") in {
                            "mention",
                            "direct_message",
                            "reply",
                        } and item.get("message_id"):
                            await self.handle(item)
                    page = result["page"]
                    if not page["has_more"]:
                        break
                    cursor = page.get("next_cursor")
                    if not cursor or cursor in seen:
                        raise SetupRequired("Ando inbox pagination did not advance")
                    seen.add(cursor)
                    args = {**args, "cursor": cursor}

    async def acknowledge(self, item, state, revision):
        result = await self.transport.call(
            "acknowledge_agent_inbox_item",
            {
                "event_id": item["event_id"],
                "expected_revision": revision,
                "state": state,
                "execution_id": self.owner,
            },
        )
        if result["status"] in {"stale_revision", "not_found"}:
            return None
        if result["status"] == "execution_conflict":
            raise SetupRequired(
                "Another receiver owns unfinished Ando work. Stop competing polling or receivers and recover the original profile; never rerun unknown tool effects"
            )
        if (
            result["status"] not in {"acknowledged", "already_acknowledged"}
            or result.get("execution_id") != self.owner
        ):
            raise SetupRequired(
                "Ando has not enabled durable receiver claims; update the backend before using this plugin"
            )
        return result.get("current_revision")

    async def history(self, item):
        recovery = item.get("recovery")
        if not recovery or recovery.get("tool") != "get_messages_by_time_range":
            # Reaction-only recovery is not a request for a generated message.
            return None
        args = dict(recovery["arguments"])
        messages = {}
        seen = set()
        while True:
            result = await self.transport.call("get_messages_by_time_range", args)
            coverage = result.get("coverage", {})
            if coverage.get("omitted_count", 0):
                raise SetupRequired(
                    "Ando history is incomplete; recover the item with full context before replying"
                )
            for message in result["items"]:
                if message.get("conversation_id") != item["conversation_id"]:
                    raise SetupRequired(
                        "Ando history returned a different conversation"
                    )
                if message.get("thread_root_message_id") == item.get(
                    "thread_root_message_id"
                ):
                    if message.get("content_truncated"):
                        full = await self.transport.call(
                            "get_message", {"message_id": message["id"]}
                        )
                        full = full.get("data", full)
                        if (
                            full.get("id") != message["id"]
                            or full.get("conversation_id") != item["conversation_id"]
                            or full.get("content_truncated")
                        ):
                            raise SetupRequired(
                                "Ando could not recover the complete original message"
                            )
                        message = {**message, **full}
                    messages[message["id"]] = message
            if len(json.dumps(list(messages.values()))) > 120000:
                raise SetupRequired(
                    "Ando recovery context exceeds the supported turn size; review this item before continuing"
                )
            page = result["page"]
            if not page["has_more"]:
                root_id = item.get("thread_root_message_id")
                if root_id and root_id not in messages:
                    root = await self.transport.call(
                        "get_message", {"message_id": root_id}
                    )
                    root = root.get("data", root)
                    if (
                        root.get("id") != root_id
                        or root.get("conversation_id") != item["conversation_id"]
                        or root.get("content_truncated")
                    ):
                        raise SetupRequired(
                            "Ando could not recover the original thread root"
                        )
                    return [root, *messages.values()]
                return list(messages.values())
            cursor = page.get("next_cursor")
            if not cursor or cursor in seen:
                raise SetupRequired("Ando history pagination did not advance")
            seen.add(cursor)
            period = result.get("period_digest", {})
            args = {**args, "cursor": cursor}
            for bound in ("start_at", "end_at"):
                if bound in period:
                    args[bound] = period[bound]

    async def handle(self, item):
        if not item.get("revision"):
            raise SetupRequired(
                "Ando must return revisioned inbox items before automatic receiving can start"
            )
        state = self.delivery.state
        # Acquire before any generation; the claim survives disconnects and new
        # messages. The same installation may resume its prepared reply.
        revision = await self.acknowledge(item, "in_progress", item["revision"])
        if revision is None:
            return
        current_item = item
        pending_key = f"pending:{item['event_id']}"
        pending = state.get(pending_key)
        if pending:
            item = json.loads(pending)
        else:
            state.set(pending_key, json.dumps(item))
        key = f"message:{item['message_id']}"
        disabled = await self.delivery.preferences()
        if {"get_message", "list_conversations"} & disabled:
            raise SetupRequired("Ando message reading is disabled in Settings > Members")
        recovery = item.get("recovery")
        if not recovery or recovery.get("tool") != "get_messages_by_time_range":
            # Revoked/inaccessible activity can have no recoverable context.
            # Retire that row without trying to read its unavailable source.
            await self.acknowledge(current_item, "read", revision)
            state.set(pending_key, "")
            return
        response = await self.transport.call(
            "get_message", {"message_id": item["message_id"]}
        )
        message = response.get("data", response)
        if (
            message.get("id") != item["message_id"]
            or message.get("conversation_id") != item["conversation_id"]
        ):
            raise SetupRequired("Ando returned a different source message")
        # Mention inbox rows group activity across a conversation. Their thread
        # field can be empty even when the latest source message is a reply.
        # The source message owns the reply destination and history boundary.
        item = {
            **item,
            "thread_root_message_id": message.get(
                "thread_root_id", item.get("thread_root_message_id")
            ),
        }
        messages = await self.history(item)
        if messages is None:
            await self.acknowledge(current_item, "read", revision)
            state.set(pending_key, "")
            return
        covered_key = f"covered:{key}"
        frozen = state.get(covered_key)
        if frozen is None:
            covered = [
                m["id"]
                for m in messages
                if m.get("authorWorkspaceMembershipId") != self.delivery.membership_id
            ]
            state.set(covered_key, json.dumps(covered))
        else:
            covered = json.loads(frozen)
        frame = {
            "payload": {
                "id": key,
                "type": "message.created",
                "workspace_id": self.delivery.workspace_id,
                "data": {
                    "object": {
                        "id": item["message_id"],
                        "conversation_id": item["conversation_id"],
                        "authorWorkspaceMembershipId": message.get(
                            "authorWorkspaceMembershipId"
                        ),
                    }
                },
                "related": {
                    "thread_root_message_id": item.get("thread_root_message_id")
                },
            }
        }
        await self.delivery.handle(frame, history=messages)
        for message_id in covered:
            state.complete(f"message:{message_id}")
        state.set(pending_key, "")
        if item["message_id"] == current_item["message_id"]:
            await self.acknowledge(current_item, "handled", revision)
        # New source activity must be processed in its own turn. Retain the
        # durable claim; the next sweep sees it with a fresh revision.
