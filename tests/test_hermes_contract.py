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


if __name__ == "__main__":
    unittest.main()
