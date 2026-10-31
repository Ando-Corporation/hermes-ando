import asyncio
import contextlib
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
            if "cursor" not in args:
                return {"items": [], "page": {"has_more": True, "next_cursor": "next"}}
            return {
                "items": []
                if self.archived
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
            self.transport, self.state, "workspace", "agent", ["human"], dispatch,
            credential_mode="invitation"
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
        # A live event only wakes the worker; its sweep never regenerates.
        self.assertTrue(self.inbox.wanted.is_set())
        await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)
        self.assertEqual(self.transport.sweeps[1]["cursor"], "next")
        # One default-scope stream: an empty inbox costs one call, not one per scope.
        self.assertNotIn("scope", self.transport.sweeps[0])
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
        await self.inbox.handle(newer)
        # The interrupted message gets its one notice, never a second generation;
        # the newer message keeps the claim for its own turn.
        sends = [args for name, args in self.transport.calls if name == "send_message"]
        self.assertEqual(len(sends), 1)
        self.assertIn("Send it again", sends[0]["markdown_content"])
        self.assertTrue(self.state.completed("message:message-2"))
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

    # Ando's current shape: one inbox item per message, recovered by its exact
    # source message (apps/convex/src/agentInbox.ts readAgentInbox).
    def message_item(self, **overrides):
        return {
            **ITEM,
            "recovery": {
                "tool": "get_message",
                "arguments": {"message_id": "message-2"},
                "coverage": "current_message_and_reactions",
            },
            **overrides,
        }

    async def test_message_item_starts_a_turn_and_is_handled(self):
        item = self.message_item()
        original = self.transport.call

        async def per_message(name, args):
            if name == "get_agent_inbox" and not self.transport.archived:
                await original(name, args)
                return {
                    "items": [copy.deepcopy(item)],
                    "page": {"has_more": False},
                }
            return await original(name, args)

        self.transport.call = per_message
        with self.assertLogs("plugin.inbox", "INFO") as logs:
            await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)
        self.assertEqual(self.turns[0]["content"], "Hello")
        self.assertTrue(self.state.completed("message:message-2"))
        self.assertFalse(self.state.completed("message:message-1"))
        acknowledgements = [args["state"] for name, args in self.transport.calls
                            if name == "acknowledge_agent_inbox_item"]
        self.assertEqual(acknowledgements, ["in_progress", "handled"])
        self.assertNotIn(
            "get_messages_by_time_range", [name for name, _ in self.transport.calls]
        )
        self.assertTrue(any("handled" in line for line in logs.output))
        # The same message arriving live never regenerates.
        await self.inbox.on_event(event(message_id="message-2"))
        # A live event only wakes the worker; its sweep never regenerates.
        self.assertTrue(self.inbox.wanted.is_set())
        await self.inbox.sweep()
        self.assertEqual(len(self.turns), 1)

    async def test_message_item_reply_includes_root_and_answers_in_thread(self):
        original = self.transport.call

        async def threaded(name, args):
            result = await original(name, args)
            if name == "get_message" and args["message_id"] == "message-2":
                result["data"]["thread_root_id"] = "root"
            return result

        self.transport.call = threaded
        await self.inbox.handle(self.message_item(type="reply"))
        self.assertIn('"id": "root"', self.turns[0]["content"])
        self.assertTrue(self.turns[0]["content"].endswith("Hello"))
        sends = [(name, args) for name, args in self.transport.calls
                 if name in {"send_message", "reply_to_message"}]
        self.assertEqual(len(sends), 1)
        self.assertEqual(sends[0][0], "reply_to_message")
        self.assertEqual(sends[0][1]["message_id"], "root")
        self.assertFalse(self.state.completed("message:root"))

    async def test_unknown_recovery_tool_is_left_unread_with_a_warning(self):
        item = self.message_item(recovery={"tool": "get_future_context", "arguments": {}})
        with self.assertLogs("plugin.inbox", "WARNING") as logs:
            await self.inbox.handle(item)
            await self.inbox.handle(item)
        self.assertEqual(self.turns, [])
        self.assertEqual(
            [name for name, _ in self.transport.calls
             if name == "acknowledge_agent_inbox_item"],
            [],
        )
        self.assertEqual(len(logs.output), 1)
        self.assertIn("get_future_context", logs.output[0])


class ApprovalTransport(FakeTransport):
    """Inbox items in order: a request whose turn blocks on approval, then replies."""

    def __init__(self, content):
        super().__init__()
        self.connection = {"receiver_id": OWNER}
        self.handled = set()
        self.content = dict(content)

    async def call(self, name, args):
        if name == "get_agent_inbox":
            items = [
                {
                    "event_id": f"inbox-{message_id}",
                    "revision": "r1",
                    "type": "direct_message",
                    "conversation_id": "conversation",
                    "message_id": message_id,
                    "thread_root_message_id": None,
                    "recovery": {"tool": "get_message", "arguments": {}},
                }
                for message_id in self.content
                if f"inbox-{message_id}" not in self.handled
            ]
            return {"items": items, "page": {"has_more": False}}
        if name == "acknowledge_agent_inbox_item":
            self.calls.append((name, copy.deepcopy(args)))
            if args["state"] == "handled":
                self.handled.add(args["event_id"])
            return {"status": "acknowledged", "execution_id": OWNER, "current_revision": "r2"}
        if name == "get_message":
            self.calls.append((name, copy.deepcopy(args)))
            return {
                "data": {
                    "id": args["message_id"],
                    "conversation_id": "conversation",
                    "authorWorkspaceMembershipId": "human",
                    "content": self.content[args["message_id"]],
                }
            }
        return await super().call(name, args)


class ApprovalReplyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = DeliveryState(Path(self.tmp.name) / "state.sqlite3", "workspace", "agent")
        self.approved = asyncio.Event()
        self.turns = []
        self.answers = []

    def tearDown(self):
        self.state.close()
        self.tmp.cleanup()

    def receiver(
        self, transport, reply_after_prompt=None, wait=5, more_replies=(), next_prompt_reply=None
    ):
        from plugin.protocol import approval_command

        async def dispatch(ref, message, disabled):
            self.turns.append(message["content"])
            if len(self.turns) == 1:
                # Hermes posts its approval prompt and the member answers at
                # once, before the prompt is even armed.
                async def post():
                    if reply_after_prompt:
                        transport.content["message-3"] = reply_after_prompt
                    for index, reply in enumerate(more_replies, 4):
                        transport.content[f"message-{index}"] = reply

                await inbox.arm_approvals(post)
                inbox.wake()
                inbox.wake()  # a second wake during the scan is not lost
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.approved.wait(), wait)
            await delivery.send(delivery.chat_id(ref), "Done")

        async def approval(ref, message):
            if approval_command(message["content"]) is None:
                return None

            async def answer():
                self.answers.append(message["content"])
                if next_prompt_reply and len(self.answers) == 1:
                    # Hermes runs on and posts its next prompt, which the
                    # member answers, before this answer returns.
                    async def post():
                        transport.content["message-9"] = next_prompt_reply

                    await inbox.arm_approvals(post)
                    return
                self.approved.set()

            return answer

        delivery = MessageDelivery(
            transport, self.state, "workspace", "agent", ["human"], dispatch,
            credential_mode="invitation",
        )
        inbox = InboxRecovery(transport, delivery, approval)
        return inbox

    async def test_reply_after_the_prompt_reaches_the_blocked_turn(self):
        transport = ApprovalTransport({"message-1": "Send the email"})
        await self.receiver(transport, reply_after_prompt="/approve").sweep()
        # One turn, for the request; the approval never started its own.
        self.assertEqual(self.turns, ["Send the email"])
        self.assertTrue(self.approved.is_set())
        self.assertEqual(transport.handled, {"inbox-message-1", "inbox-message-3"})
        self.assertTrue(self.state.completed("message:message-3"))

    async def test_one_prompt_takes_one_reply(self):
        # Two approvals queued for one prompt. Hermes then waits on a second
        # command without posting a prompt Ando saw; "yes" must not approve it.
        transport = ApprovalTransport({"message-1": "Send the email"})
        inbox = self.receiver(
            transport, reply_after_prompt="/approve", more_replies=("yes",)
        )
        await inbox.sweep()
        self.assertEqual(self.answers, ["/approve"])
        # The extra "yes" is answered as an ordinary message after the turn.
        await inbox.sweep()
        self.assertEqual(self.turns, ["Send the email", "yes"])

    async def test_next_prompt_posted_during_an_answer_takes_its_reply(self):
        transport = ApprovalTransport({"message-1": "Send the email"})
        await self.receiver(
            transport, reply_after_prompt="/approve", next_prompt_reply="/deny"
        ).sweep()
        self.assertEqual(self.answers, ["/approve", "/deny"])
        self.assertEqual(self.turns, ["Send the email"])

    async def test_reply_waiting_before_the_prompt_never_approves(self):
        transport = ApprovalTransport({"message-1": "Send the email", "message-2": "yes"})
        await self.receiver(transport, wait=0.05).sweep()
        self.assertFalse(self.approved.is_set())
        # The earlier "yes" is answered as an ordinary message after the turn.
        self.assertEqual(self.turns, ["Send the email", "yes"])

    async def test_ordinary_reply_waits_for_the_turn(self):
        transport = ApprovalTransport({"message-1": "Send the email"})
        inbox = self.receiver(transport, reply_after_prompt="also cc Dana", wait=0.05)
        await inbox.sweep()
        self.assertFalse(self.approved.is_set())
        # Its wake runs the worker's next sweep, which answers it in its own turn.
        self.assertTrue(inbox.wanted.is_set())
        await inbox.sweep()
        self.assertEqual(self.turns, ["Send the email", "also cc Dana"])


class ApprovalCommandTests(unittest.TestCase):
    def test_only_approval_replies_become_commands(self):
        from plugin.protocol import approval_command

        self.assertEqual(approval_command("approve"), "/approve")
        self.assertEqual(approval_command(" Approve  always "), "/approve always")
        self.assertEqual(approval_command("/approve"), "/approve")
        self.assertEqual(approval_command("/deny too risky"), "/deny too risky")
        for text in (None, "", "approve it please", "/restart", "yes, but later"):
            self.assertIsNone(approval_command(text))
