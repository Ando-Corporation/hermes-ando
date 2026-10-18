"""Run against an explicit upstream Hermes checkout, never a fake BasePlatformAdapter.

HERMES_SOURCE=/path/to/verified/hermes python -m unittest discover ...
Without that checkout these provider compatibility tests are reported skipped.
"""

import asyncio
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_delivery import FakeTransport, event


def hermes_has_display_tiers():
    import inspect
    from gateway.platform_registry import PlatformEntry

    return "display_tier" in inspect.signature(PlatformEntry).parameters


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE to run real Hermes lifecycle compatibility tests",
)
class HermesContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"HERMES_HOME": self.tmp.name})
        self.env.start()
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        from gateway.platform_registry import PlatformEntry, platform_registry
        from gateway.config import PlatformConfig
        from plugin.adapter import AndoAdapter, register
        from plugin.delivery import MessageDelivery
        from plugin.state import DeliveryState

        register(
            SimpleNamespace(
                register_platform=lambda **kw: platform_registry.register(
                    PlatformEntry(**kw)
                )
            )
        )
        self.adapter = AndoAdapter(
            PlatformConfig(
                enabled=True,
                typing_indicator=False,
                gateway_restart_notification=False,
                extra={
                    "workspace_id": "workspace",
                    "membership_id": "agent",
                    "mcp_server": "ando",
                    "allowed_users": ["human"],
                    "group_sessions_per_user": False,
                },
            )
        )
        self.state = DeliveryState(
            Path(self.tmp.name) / "delivery.sqlite3", "workspace", "agent"
        )
        self.transport = FakeTransport()
        self.adapter._delivery = MessageDelivery(
            self.transport,
            self.state,
            "workspace",
            "agent",
            ["human"],
            self.adapter._dispatch,
        )

    async def asyncTearDown(self):
        await self.adapter.disconnect()
        self.state.close()
        from gateway.status import flush_runtime_status_async

        self.assertTrue(await flush_runtime_status_async())
        self.env.stop()
        # Current Hermes persists turn markers through asyncio.to_thread.
        # Drain the isolated loop's executor before removing its profile home.
        await asyncio.get_running_loop().shutdown_default_executor()
        self.tmp.cleanup()

    async def test_second_receiver_for_the_same_agent_is_refused(self):
        self.adapter.config.extra.update(
            credential_mode="invitation", installer_id="human"
        )
        with patch(
            "gateway.status.acquire_scoped_lock",
            return_value=(False, {"pid": 1}),
        ):
            self.assertFalse(await self.adapter.connect())
        self.assertEqual(self.adapter.fatal_error_code, "lock_conflict")

    def test_registration_without_display_tiers_still_loads(self):
        # Hermes before v0.21.6 rejects display_tier; the plugin must still
        # register there (noisier) instead of failing to load.
        import dataclasses
        from gateway import platform_registry
        from plugin.adapter import register

        older = dataclasses.make_dataclass(
            "PlatformEntry",
            [
                (f.name, f.type, f)
                for f in dataclasses.fields(platform_registry.PlatformEntry)
                if f.name != "display_tier"
            ],
        )
        registered = []
        with (
            patch.object(platform_registry, "PlatformEntry", older),
            self.assertLogs("plugin.adapter", "WARNING") as logs,
        ):
            register(SimpleNamespace(register_platform=lambda **kw: registered.append(older(**kw))))
        self.assertEqual(registered[0].name, "ando")
        self.assertIn("update Hermes", logs.output[0])
        if hermes_has_display_tiers():
            register(SimpleNamespace(register_platform=lambda **kw: registered.append(kw)))
            self.assertEqual(registered[1]["display_tier"], "low")

    async def test_disconnect_releases_the_receiver_lock(self):
        # Hermes moved release_scoped_lock out of gateway.status; a failed
        # import here left the gateway unable to finish disconnecting.
        from gateway.status import acquire_scoped_lock
        from plugin.protocol import lock_acquired

        locks = Path(self.tmp.name) / "locks"
        with patch.dict(os.environ, {"HERMES_GATEWAY_LOCK_DIR": str(locks)}):
            self.adapter._lock_key = "workspace:agent"
            self.assertTrue(lock_acquired(acquire_scoped_lock("ando", "workspace:agent")))
            self.assertEqual(len(list(locks.glob("*.lock"))), 1)
            await self.adapter.disconnect()
            self.assertEqual(list(locks.glob("*.lock")), [])
        self.assertIsNone(self.adapter._lock_key)

    async def test_idle_delivery_starts_real_background_turn_and_waits_for_confirmed_reply(
        self,
    ):
        started = asyncio.Event()
        finish = asyncio.Event()

        async def handler(incoming):
            self.assertFalse(incoming.allow_gateway_control)
            self.assertTrue(incoming.source.role_authorized)
            started.set()
            await finish.wait()
            return "A real Hermes lifecycle reply"

        self.adapter.set_message_handler(handler)
        delivery = asyncio.create_task(self.adapter._delivery.handle(event()))
        await asyncio.wait_for(started.wait(), 5)
        self.assertFalse(delivery.done())
        self.assertFalse(self.state.completed("event-1"))
        finish.set()
        await asyncio.wait_for(delivery, 5)
        self.assertTrue(self.state.completed("event-1"))
        self.assertTrue(any(name == "send_message" for name, _ in self.transport.calls))

    async def test_followups_share_session_and_other_threads_stay_isolated(self):
        keys = []

        async def handler(incoming):
            keys.append(self.adapter._event_session_key(incoming))
            return "reply"

        self.adapter.set_message_handler(handler)
        for frame in [event(), event("e2", "m2"), event("e3", "m3", "root")]:
            await asyncio.wait_for(self.adapter._delivery.handle(frame), 5)
            # Let the real base-class task release its guard after the lifecycle hook.
            await asyncio.sleep(0)
        self.assertEqual(keys[0], keys[1])
        self.assertNotEqual(keys[1], keys[2])

    async def test_registered_tier_keeps_ando_quiet_unless_the_operator_opts_in(self):
        if not hermes_has_display_tiers():
            self.skipTest("This Hermes predates display tiers (v0.21.6)")
        from gateway.display_config import resolve_display_setting

        quiet = {
            "tool_progress": "off",
            "interim_assistant_messages": False,
            "long_running_notifications": False,
            "busy_ack_detail": False,
            "streaming": False,
        }
        for setting, value in quiet.items():
            self.assertEqual(resolve_display_setting({}, "ando", setting), value)
        opted_in = {"display": {"platforms": {"ando": {"long_running_notifications": True}}}}
        self.assertTrue(
            resolve_display_setting(opted_in, "ando", "long_running_notifications")
        )

    async def test_interim_sends_and_statuses_never_become_the_reply(self):
        from gateway.run import _interim_metadata

        edits = {}

        async def handler(incoming):
            chat = incoming.source.chat_id
            beat = await self.adapter.send(
                chat, "⏳ Working — 3 min", metadata=_interim_metadata()
            )
            edited = await self.adapter.edit_message(chat, beat.message_id, "⏳ Working — 6 min")
            edits["heartbeat"] = edited.success
            first = await self.adapter.send_or_update_status(chat, "context", "Compressing")
            again = await self.adapter.send_or_update_status(chat, "context", "Compressed")
            edits["status"] = again.message_id == first.message_id
            edits["foreign"] = (
                await self.adapter.edit_message(chat, "someone-else", "x")
            ).success
            return "A real Hermes lifecycle reply"

        self.adapter.set_message_handler(handler)
        await asyncio.wait_for(self.adapter._delivery.handle(event()), 5)
        self.assertEqual(edits, {"heartbeat": True, "status": True, "foreign": False})
        self.assertEqual(
            [(tool, args["markdown_content"]) for _, tool, args, _ in self.state.replies("event-1")],
            [("send_message", "A real Hermes lifecycle reply")],
        )
        self.assertEqual(self.transport.posted, 2)
        self.assertTrue(self.state.completed("event-1"))
        # Statuses are not editable after their turn.
        late = await self.adapter.edit_message(
            "workspace/agent/conversation", "status-1", "late"
        )
        self.assertFalse(late.success)

    async def test_gateway_setup_notices_stay_out_of_ando_and_the_reply(self):
        from gateway.run_notifications import GatewayNotificationsMixin

        runner = SimpleNamespace(
            config=None,
            _delivery_adapter_for=lambda source: self.adapter,
            _thread_metadata_for_source=lambda source: None,
        )

        async def handler(incoming):
            # The one-time "No home channel is set... type /sethome" prompt.
            await GatewayNotificationsMixin._deliver_platform_notice(
                runner, incoming.source, "📬 No home channel is set for Ando."
            )
            return "A real Hermes lifecycle reply"

        self.adapter.set_message_handler(handler)
        with self.assertLogs("plugin.adapter", "WARNING"):
            await asyncio.wait_for(self.adapter._delivery.handle(event()), 5)
        self.assertEqual(
            [args["markdown_content"] for _, _, args, _ in self.state.replies("event-1")],
            ["A real Hermes lifecycle reply"],
        )
        self.assertEqual(self.transport.posted, 0)

    async def test_gateway_failure_does_not_complete_delivery(self):
        async def handler(_):
            raise RuntimeError("simulated provider failure")

        self.adapter.set_message_handler(handler)
        with self.assertRaisesRegex(RuntimeError, "did not complete"):
            await asyncio.wait_for(self.adapter._delivery.handle(event()), 5)
        self.assertFalse(self.state.completed("event-1"))
        self.assertEqual(self.state.replies("event-1"), [])

    async def test_no_handler_fails_instead_of_silently_acknowledging(self):
        with self.assertRaisesRegex(RuntimeError, "message handler"):
            await asyncio.wait_for(self.adapter._delivery.handle(event()), 5)

    async def test_invitation_defaults_private_and_explicit_widening(self):
        from plugin.adapter import local_allowed_users
        with patch.dict(os.environ, {}, clear=True):
            extra = {"installer_id": "human", "credential_mode": "invitation"}
            self.assertEqual(local_allowed_users(extra), ["human"])
            self.assertEqual(local_allowed_users({**extra, "allowed_users": []}), [])
            self.assertEqual(local_allowed_users({**extra, "allowed_users": ["*"]}), ["*"])
            with patch.dict(os.environ, {"ANDO_ALLOWED_USERS": "other, agent-2"}):
                self.assertEqual(local_allowed_users(extra), ["other", "agent-2"])
            with patch.dict(os.environ, {"ANDO_ALLOW_ALL_USERS": "true"}):
                self.assertEqual(local_allowed_users(extra), ["*"])

    async def test_unknown_sender_cannot_start_host_turn(self):
        calls = []
        async def handler(incoming):
            calls.append(incoming)
            return "reply"
        self.adapter.set_message_handler(handler)
        frame = event()
        frame["payload"]["data"]["object"]["authorWorkspaceMembershipId"] = "other-agent"
        await self.adapter._delivery.handle(frame)
        self.assertEqual(calls, [])
        self.assertFalse(any(name in {"send_message", "reply_to_message"}
                             for name, _ in self.transport.calls))

    async def test_unchecked_source_does_not_delegate_authorization(self):
        seen = []
        async def handler(incoming):
            seen.append(incoming.source.role_authorized)
            return "reply"
        self.adapter.set_message_handler(handler)
        # Defense in depth: even if intake is accidentally wider, the source
        # must not assert a local grant for an author absent from config.
        self.adapter._delivery.allowed_users = None
        original = self.transport.call
        async def other_source(name, args):
            result = await original(name, args)
            if name == "get_message":
                result["data"]["authorWorkspaceMembershipId"] = "other-agent"
            return result
        self.transport.call = other_source
        frame = event()
        frame["payload"]["data"]["object"]["authorWorkspaceMembershipId"] = "other-agent"
        await self.adapter._delivery.handle(frame)
        self.assertEqual(seen, [False])


if __name__ == "__main__":
    unittest.main()
