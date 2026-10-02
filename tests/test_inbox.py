import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.inbox import InboxRecovery
from plugin.delivery import MessageDelivery
from plugin.protocol import SetupRequired
from plugin.state import DeliveryState
from test_delivery import FakeTransport, event

OWNER = "11111111-1111-4111-8111-111111111111"
ITEM = {
    "event_id": "inbox-1",
    "revision": "r1",
    "type": "direct_message",
    "conversation_id": "conversation",
    "message_id": "message-2",
    "thread_root_message_id": None,
    "recovery": {
        "tool": "get_messages_by_time_range",
        "arguments": {"conversation_id": "conversation", "start_at": 1, "end_at": 5},
    },
}


class Transport(FakeTransport):
    def __init__(self):
        super().__init__()
        self.connection = {"receiver_id": OWNER}
        self.archived = False
        self.conflict = False
        self.stale = False
        self.sweeps = []

    async def call(self, name, args):
        if name == "get_agent_inbox":
            self.sweeps.append(copy.deepcopy(args))
            # Empty first page must still be followed.
            if args["scope"] == "direct" and "cursor" not in args:
                return {"items": [], "page": {"has_more": True, "next_cursor": "next"}}
            return {
                "items": []
                if self.archived or args["scope"] == "updates"
                else [copy.deepcopy(ITEM)],
                "page": {"has_more": False},
            }
        if name == "acknowledge_agent_inbox_item":
            self.calls.append((name, copy.deepcopy(args)))
            if self.conflict:
                return {"status": "execution_conflict"}
            if self.stale:
                return {"status": "stale_revision"}
            if args["state"] == "handled":
                self.archived = True
            return {
                "status": "acknowledged",
                "execution_id": OWNER,
                "current_revision": "r2",
            }
        if name == "get_messages_by_time_range":
            number = "1" if "cursor" not in args else "2"
            return {
                "items": [
                    {
                        "id": "message-" + number,
                        "conversation_id": "conversation",
                        "authorWorkspaceMembershipId": "human",
                        "thread_root_message_id": None,
                        "content": "request " + number,
                    }
                ],
                "coverage": {"omitted_count": 0, "content_truncated": False},
                "page": {
                    "has_more": number == "1",
                    "next_cursor": "history-next" if number == "1" else None,
                },
            }
        return await super().call(name, args)


class InboxTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = DeliveryState(
            Path(self.tmp.name) / "state.sqlite3", "workspace", "agent"
        )
        self.transport = Transport()
        self.turns = []

        async def dispatch(ref, message, disabled):
            self.turns.append(message)
            await self.delivery.send(self.delivery.chat_id(ref), "Done")

        self.delivery = MessageDelivery(
            self.transport, self.state, "workspace", "agent", None, dispatch
        )
        self.inbox = InboxRecovery(self.transport, self.delivery)

    def tearDown(self):
        self.state.close()
        self.tmp.cleanup()

    async def test_recovery_follows_all_pages_and_live_overlap_never_regenerates(self):
        await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)
        self.assertIn("request 1", self.turns[0]["content"])
        self.assertIn("request 2", self.turns[0]["content"])
        self.assertTrue(self.state.completed("message:message-1"))
        self.assertTrue(self.state.completed("message:message-2"))
        await self.inbox.on_event(event(message_id="message-1"))
        self.assertEqual(len(self.turns), 1)
        self.assertEqual(self.transport.sweeps[1]["cursor"], "next")
        self.assertTrue(self.transport.archived)

    async def test_competing_receiver_never_generates(self):
        self.transport.conflict = True
        with self.assertRaises(SetupRequired):
            await self.inbox.sweep()
        self.assertEqual(self.turns, [])

    async def test_stale_claim_waits_for_a_fresh_sweep(self):
        self.transport.stale = True
        await self.inbox.sweep()
        self.assertEqual(self.turns, [])

    async def test_uncertain_send_replays_frozen_content_without_new_generation(self):
        self.transport.fail_send = True
        with self.assertRaises(OSError):
            await self.inbox.sweep()
        self.assertFalse(self.transport.archived)
        self.transport.fail_send = False
        await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)
        self.assertTrue(self.transport.archived)

    async def test_incomplete_history_does_not_generate_or_archive(self):
        original = self.transport.call

        async def incomplete(name, args):
            result = await original(name, args)
            if name == "get_messages_by_time_range":
                result["coverage"]["omitted_count"] = 1
            return result

        self.transport.call = incomplete
        with self.assertRaises(SetupRequired):
            await self.inbox.sweep()
        self.assertEqual(self.turns, [])
        self.assertFalse(self.transport.archived)

    async def test_new_message_does_not_bypass_interrupted_generation(self):
        async def interrupted(*args):
            raise RuntimeError("provider stopped after tool effects")

        self.delivery.dispatch = interrupted
        with self.assertRaises(RuntimeError):
            await self.inbox.handle(copy.deepcopy(ITEM))
        newer = {**ITEM, "message_id": "message-3", "revision": "newer"}
        with self.assertRaisesRegex(SetupRequired, "message:message-2.*interrupted"):
            await self.inbox.handle(newer)
        self.assertFalse(self.transport.archived)
        self.assertFalse(self.state.started("message:message-3"))

    async def test_history_budget_pagination_is_not_missing_content(self):
        original = self.transport.call

        async def truncated_page(name, args):
            result = await original(name, args)
            if name == "get_messages_by_time_range":
                result["coverage"]["content_truncated"] = result["page"]["has_more"]
            return result

        self.transport.call = truncated_page
        await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)
        self.assertIn("request 2", self.turns[0]["content"])

    async def test_thread_recovery_fetches_original_root_and_replies_in_thread(self):
        item = {**ITEM, "type": "reply", "thread_root_message_id": "root"}
        original = self.transport.call

        async def threaded(name, args):
            result = await original(name, args)
            if name == "get_messages_by_time_range":
                for message in result["items"]:
                    message["thread_root_message_id"] = "root"
            return result

        self.transport.call = threaded
        await self.inbox.handle(item)
        self.assertIn('"id": "root"', self.turns[0]["content"])
        sends = [
            args for name, args in self.transport.calls if name == "reply_to_message"
        ]
        self.assertEqual(sends[0]["message_id"], "root")

    async def test_thread_mention_uses_source_root_when_group_has_no_root(self):
        original = self.transport.call

        async def threaded(name, args):
            result = await original(name, args)
            if name == "get_message" and args["message_id"] == "message-2":
                result["data"]["thread_root_id"] = "root"
            if name == "get_messages_by_time_range":
                for message in result["items"]:
                    if message["id"] == "message-2":
                        message["thread_root_message_id"] = "root"
            return result

        self.transport.call = threaded
        await self.inbox.handle({**ITEM, "type": "mention"})
        self.assertIn('"id": "root"', self.turns[0]["content"])
        self.assertIn("request 2", self.turns[0]["content"])
        self.assertNotIn("request 1", self.turns[0]["content"])
        self.assertFalse(self.state.completed("message:message-1"))
        sends = [(name, args) for name, args in self.transport.calls
                 if name in {"send_message", "reply_to_message"}]
        self.assertEqual(len(sends), 1)
        self.assertEqual(sends[0][0], "reply_to_message")
        self.assertEqual(sends[0][1]["message_id"], "root")

    async def test_unrecoverable_item_is_read_without_fetching_inaccessible_source(self):
        original = self.transport.call

        async def inaccessible(name, args):
            if name == "get_message":
                raise PermissionError("conversation access revoked")
            return await original(name, args)

        self.transport.call = inaccessible
        await self.inbox.handle({**ITEM, "recovery": None})
        self.assertEqual(self.turns, [])
        self.assertEqual(self.state.get("pending:inbox-1"), "")
        acknowledgements = [args["state"] for name, args in self.transport.calls
                            if name == "acknowledge_agent_inbox_item"]
        self.assertEqual(acknowledgements, ["in_progress", "read"])
