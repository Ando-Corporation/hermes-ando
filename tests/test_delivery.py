import asyncio
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.delivery import MessageDelivery
from plugin.protocol import (
    SetupRequired,
    verify_identity,
    verify_ticket,
    verify_workspace_info,
)
from plugin.state import DeliveryState
from plugin.transport import receive_socket
from server_fixtures import conversation_directory


def event(event_id="event-1", message_id="message-1", thread=None):
    return {
        "type": "event",
        "envelope_id": f"envelope-{event_id}",
        "cursor": "UNSAFE-EVENT-CURSOR",
        "payload": {
            "id": event_id,
            "type": "message.created",
            "workspace_id": "workspace",
            "data": {
                "object": {
                    "id": message_id,
                    "conversation_id": "conversation",
                    "authorWorkspaceMembershipId": "human",
                }
            },
            "related": {"thread_root_message_id": thread},
        },
    }


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.fail_send = False
        self.preferences = {"disabled_tool_ids": [], "tools": []}

    async def call(self, name, args):
        self.calls.append((name, copy.deepcopy(args)))
        if name == "share_agent_resources":
            return copy.deepcopy(self.preferences)
        if name == "list_conversations":
            return conversation_directory("conversation")
        if name == "get_message":
            return {
                "data": {
                    "id": args["message_id"],
                    "conversation_id": "conversation",
                    "authorWorkspaceMembershipId": "human",
                    "content": "Hello",
                }
            }
        if name in {"send_message", "reply_to_message"}:
            if self.fail_send:
                raise OSError("uncertain network result")
            return {"message_id": "reply-1"}
        raise AssertionError(name)


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite3"
        self.state = DeliveryState(self.path, "workspace", "agent")
        self.transport = FakeTransport()
        self.dispatch_count = 0

    def tearDown(self):
        self.state.close()
        self.tmp.cleanup()

    def delivery(self, dispatch=None):
        async def default_dispatch(ref, message, disabled):
            self.dispatch_count += 1
            await delivery.send(delivery.chat_id(ref), "Hello back")

        delivery = MessageDelivery(
            self.transport,
            self.state,
            "workspace",
            "agent",
            ["human"],
            dispatch or default_dispatch,
        )
        return delivery

    async def test_duplicate_delivery_after_restart_does_not_run_or_send_again(self):
        await self.delivery().handle(event())
        self.state.close()
        self.state = DeliveryState(self.path, "workspace", "agent")
        await self.delivery().handle(event())
        self.assertEqual(self.dispatch_count, 1)
        self.assertEqual(
            len([call for call in self.transport.calls if call[0] == "send_message"]), 1
        )

    async def test_uncertain_write_retries_identical_content_and_key_without_regeneration(
        self,
    ):
        self.transport.fail_send = True
        with self.assertRaises(OSError):
            await self.delivery().handle(event())
        self.state.close()
        self.state = DeliveryState(self.path, "workspace", "agent")
        self.transport.fail_send = False
        await self.delivery().handle(event())
        sends = [args for name, args in self.transport.calls if name == "send_message"]
        self.assertEqual(sends[0], sends[1])
        self.assertEqual(self.dispatch_count, 1)
        self.assertTrue(self.state.completed("event-1"))

    async def test_thread_reply_uses_root_and_followups_keep_conversation_scope(self):
        delivery = self.delivery()
        await delivery.handle(event(thread="root"))
        await delivery.handle(event("event-2", "message-2", thread="root"))
        sends = [
            args for name, args in self.transport.calls if name == "reply_to_message"
        ]
        self.assertEqual([args["message_id"] for args in sends], ["root", "root"])
        self.assertNotEqual(sends[0]["idempotency_key"], sends[1]["idempotency_key"])

    async def test_turn_admission_without_reply_never_completes_event(self):
        async def no_reply(*_):
            pass

        with self.assertRaisesRegex(RuntimeError, "without a confirmed"):
            await self.delivery(no_reply).handle(event())
        self.assertFalse(self.state.completed("event-1"))

    async def test_wrong_workspace_fails_before_any_tool_call(self):
        frame = event()
        frame["payload"]["workspace_id"] = "other"
        with self.assertRaises(SetupRequired):
            await self.delivery().handle(frame)
        self.assertEqual(self.transport.calls, [])

    async def test_self_and_nonallowlisted_senders_do_not_start_model(self):
        for author in ["agent", "stranger"]:
            frame = event(author)
            frame["payload"]["data"]["object"]["authorWorkspaceMembershipId"] = author
            await self.delivery().handle(frame)
        self.assertEqual(self.dispatch_count, 0)
        self.assertEqual(self.transport.calls, [])

    async def test_tool_preferences_are_refetched_before_followup(self):
        delivery = self.delivery()
        await delivery.handle(event())
        self.transport.preferences = {
            "disabled_tool_ids": ["ando:send"],
            "tools": [
                {
                    "id": "ando:send",
                    "name": "send_message",
                    "description": "Reply in Ando",
                    "enabled": False,
                }
            ],
        }
        with self.assertRaisesRegex(SetupRequired, "replies are disabled"):
            await delivery.handle(event("event-2", "message-2"))
        self.assertEqual(self.dispatch_count, 1)
        self.assertFalse(self.state.started("event-2"))
        self.assertFalse(self.state.completed("event-2"))

    async def test_unsolicited_or_cross_conversation_sends_are_rejected(self):
        delivery = self.delivery()
        with self.assertRaises(SetupRequired):
            await delivery.send("another-conversation", "hi")

    async def test_disabled_host_tool_prevents_generation_entirely(self):
        self.transport.preferences = {
            "disabled_tool_ids": ["hermes:terminal"],
            "tools": [
                {
                    "id": "hermes:terminal",
                    "name": "terminal",
                    "description": "Run a command",
                    "enabled": False,
                }
            ],
        }
        with self.assertRaisesRegex(SetupRequired, "cannot filter per turn"):
            await self.delivery().handle(event())
        self.assertEqual(self.dispatch_count, 0)
        self.assertFalse(self.state.started("event-1"))

    async def test_malformed_preferences_never_start_generation(self):
        tool = {
            "id": "ando:send",
            "name": "send_message",
            "description": "Reply in Ando",
            "enabled": False,
        }
        invalid = [
            None,
            [],
            {},
            {"tools": []},
            {"disabled_tool_ids": None, "tools": []},
            {"disabled_tool_ids": "ando:send", "tools": []},
            {"disabled_tool_ids": [1], "tools": []},
            {"disabled_tool_ids": [""], "tools": []},
            {"disabled_tool_ids": []},
            {"disabled_tool_ids": [], "tools": [None]},
            {"disabled_tool_ids": ["ando:send"], "tools": []},
            {"disabled_tool_ids": ["send_message"], "tools": []},
            {"disabled_tool_ids": [], "tools": [tool]},
            {"disabled_tool_ids": ["ando:send"], "tools": [{**tool, "enabled": True}]},
            {"disabled_tool_ids": ["ando:send"], "tools": [{**tool, "name": 1}]},
            {
                "disabled_tool_ids": ["ando:send"],
                "tools": [{**tool, "description": None}],
            },
            {
                "disabled_tool_ids": ["ando:send"],
                "tools": [tool, {**tool, "name": "get_message"}],
            },
        ]
        for index, preferences in enumerate(invalid):
            event_id = f"malformed-{index}"
            with self.subTest(preferences=preferences):
                self.transport.preferences = preferences
                with self.assertRaisesRegex(
                    SetupRequired, "malformed tool preferences"
                ):
                    await self.delivery().handle(event(event_id))
                self.assertFalse(self.state.started(event_id))
                self.assertFalse(self.state.completed(event_id))
        self.assertEqual(self.dispatch_count, 0)
        self.assertTrue(
            all(name == "share_agent_resources" for name, _ in self.transport.calls)
        )

    async def test_interrupted_generation_requires_review_instead_of_repeating_tools(
        self,
    ):
        async def crash_before_reply(*_):
            self.dispatch_count += 1
            raise RuntimeError("tool may have run before crash")

        with self.assertRaises(RuntimeError):
            await self.delivery(crash_before_reply).handle(event())
        self.state.close()
        self.state = DeliveryState(self.path, "workspace", "agent")
        with self.assertRaisesRegex(SetupRequired, "interrupted"):
            await self.delivery().handle(event())
        self.assertEqual(self.dispatch_count, 1)
        self.state.retry_after_review("event-1")
        await self.delivery().handle(event())
        self.assertEqual(self.dispatch_count, 2)

    async def test_recovery_cannot_clear_frozen_or_completed_reply(self):
        await self.delivery().handle(event())
        with self.assertRaises(ValueError):
            self.state.retry_after_review("event-1")

    def test_state_cannot_be_reused_for_another_agent(self):
        with self.assertRaises(ValueError):
            DeliveryState(self.path, "workspace", "another-agent")


class FakeSocket:
    def __init__(self, frames):
        self.frames = frames
        self.sent = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.frames:
            raise StopAsyncIteration
        return json.dumps(self.frames.pop(0))

    async def send(self, text):
        self.sent.append(json.loads(text))


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_cursor_is_never_persisted_and_ack_waits_for_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = DeliveryState(Path(tmp) / "state", "workspace", "agent")
            state.set("cursor", "initial")
            socket = FakeSocket(
                [
                    event(),
                    {
                        "type": "acknowledged",
                        "envelope_id": "envelope-event-1",
                        "resume_cursor": "confirmed",
                    },
                ]
            )

            async def handle(_):
                self.assertEqual(state.get("cursor"), "initial")
                self.assertEqual(socket.sent, [])
                await asyncio.sleep(0)

            await receive_socket(socket, state, handle)
            self.assertEqual(state.get("cursor"), "confirmed")
            self.assertEqual(socket.sent, [{"envelope_id": "envelope-event-1"}])
            state.close()

    async def test_failed_handler_nacks_and_preserves_resume_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = DeliveryState(Path(tmp) / "state", "workspace", "agent")
            state.set("cursor", "safe")
            socket = FakeSocket([event()])

            async def handle(_):
                raise RuntimeError("runtime failed")

            with self.assertRaises(RuntimeError):
                await receive_socket(socket, state, handle)
            self.assertEqual(state.get("cursor"), "safe")
            self.assertEqual(socket.sent[0]["error"]["code"], "handler_failed")
            state.close()

    async def test_planned_rotation_uses_server_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = DeliveryState(Path(tmp) / "state", "workspace", "agent")
            socket = FakeSocket(
                [
                    {
                        "type": "disconnect",
                        "reason": "connection_max_age",
                        "resume_cursor": "rotation",
                        "retry_after_seconds": 2,
                    }
                ]
            )
            self.assertEqual(await receive_socket(socket, state, None), 2)
            self.assertEqual(state.get("cursor"), "rotation")
            state.close()

    def test_identity_and_ticket_are_pinned_to_the_requested_agent(self):
        identity = {
            "identity": {
                "identity_type": "agent",
                "workspace_id": "workspace",
                "workspace_membership_id": "agent",
                "display_name": "Ada",
            }
        }
        verify_identity(identity, "workspace", "agent", "Ada")
        with self.assertRaises(SetupRequired):
            verify_identity(identity, "workspace", "other")
        ticket = {
            "url": "wss://mcp.ando.so/link?ticket=secret",
            "protocol": "ando.realtime.v1",
            "resume_cursor": "safe",
            "subscriptions": [
                {"target": {"id": "agent", "type": "workspace_membership"}}
            ],
        }
        verify_ticket(ticket, "https://mcp.ando.so", "agent")
        ticket["url"] = "wss://other.invalid/link?ticket=secret"
        with self.assertRaises(SetupRequired):
            verify_ticket(ticket, "https://mcp.ando.so", "agent")

    def test_reconnect_rejects_missing_or_changed_installer(self):
        info = {
            "workspace": {"workspace_id": "workspace"},
            "installed_by": {"workspace_membership_id": "installer"},
        }
        verify_workspace_info(info, "workspace", "installer")
        for installer in [None, {"workspace_membership_id": "stranger"}]:
            with self.assertRaises(SetupRequired):
                verify_workspace_info(
                    {**info, "installed_by": installer}, "workspace", "installer"
                )


if __name__ == "__main__":
    unittest.main()
