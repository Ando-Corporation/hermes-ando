"""Files both ways through a real Hermes gateway lifecycle (HERMES_SOURCE checkout).

HERMES_SOURCE=/path/to/verified/hermes python -m unittest discover ...
"""

import asyncio
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_delivery import event
from test_delivery_files import ClosingHttp, FilesTransport
from test_files import PDF, PNG


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE to run real Hermes lifecycle compatibility tests",
)
class HermesFilesContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"HERMES_HOME": self.tmp.name})
        self.env.start()
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        from gateway.config import PlatformConfig
        from gateway.platform_registry import PlatformEntry, platform_registry
        from plugin.adapter import AndoAdapter, register
        from plugin.delivery import MessageDelivery
        from plugin.state import DeliveryState

        register(SimpleNamespace(register_platform=lambda **kw: platform_registry.register(PlatformEntry(**kw))))
        self.adapter = AndoAdapter(PlatformConfig(
            enabled=True, typing_indicator=False, gateway_restart_notification=False,
            extra={"workspace_id": "workspace", "membership_id": "agent", "mcp_server": "ando",
                   "allowed_users": ["human"], "group_sessions_per_user": False}))
        self.state = DeliveryState(self.home / "delivery.sqlite3", "workspace", "agent")
        self.transport = FilesTransport()
        self.http = ClosingHttp(self.transport.ando)
        self.client = patch("plugin.files._client", lambda: self.http)
        self.client.start()
        self.adapter._delivery = MessageDelivery(
            self.transport, self.state, "workspace", "agent", ["human"], self.adapter._dispatch)

    async def asyncTearDown(self):
        await self.adapter.disconnect()
        self.client.stop()
        self.state.close()
        from gateway.status import flush_runtime_status_async

        self.assertTrue(await flush_runtime_status_async())
        self.env.stop()
        await asyncio.get_running_loop().shutdown_default_executor()
        self.tmp.cleanup()

    def cached(self, folder, name, data):
        path = self.home / "cache" / folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    async def turn(self, response):
        received = []

        async def handler(incoming):
            received.append(incoming)
            return response
        self.adapter.set_message_handler(handler)
        await asyncio.wait_for(self.adapter._delivery.handle(event()), 10)
        return received[0]

    def replies(self):
        return [{k: v for k, v in args.items() if k not in {"idempotency_key", "conversation_id"}}
                for _, _, args, message_id in self.state.replies("event-1") if message_id]

    async def test_final_text_then_document_are_two_confirmed_replies(self):
        pdf = self.cached("documents", "report.pdf", PDF)
        await self.turn(f"Here is the report.\nMEDIA:{pdf}")
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies(), [{"markdown_content": "Here is the report."}, {"file_ids": ["file-1"]}])
        self.assertEqual(self.transport.ando.stored["file-1"], PDF)

    async def test_media_only_turn_succeeds_with_the_attachment_as_its_reply(self):
        png = self.cached("images", "chart.png", PNG)
        await self.turn(f"MEDIA:{png}")
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies(), [{"file_ids": ["file-1"]}])

    async def test_markdown_remote_image_is_downloaded_then_attached(self):
        png = self.cached("images", "remote.png", PNG)

        async def downloaded(url, ext=".jpg", retries=2):
            self.assertEqual(url, "https://images.example/chart.png")
            return str(png)
        with patch("gateway.platforms.base.cache_image_from_url", downloaded):
            await self.turn("Chart: ![Revenue](https://images.example/chart.png)")
        self.assertEqual(self.replies(), [{"markdown_content": "Chart:"},
                                          {"markdown_content": "Revenue", "file_ids": ["file-1"]}])

    async def test_a_batch_with_a_failed_image_says_so_and_still_succeeds(self):
        png = self.cached("images", "ok.png", PNG)

        async def download(url, ext=".jpg", retries=2):
            if "broken" in url:
                raise ValueError("Refusing to cache non-image data")
            return str(png)
        with patch("gateway.platforms.base.cache_image_from_url", download):
            await self.turn("![a](https://images.example/a.png) ![b](https://images.example/broken.png)")
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies(), [
            {"markdown_content": "a", "file_ids": ["file-1"]},
            {"markdown_content": "⚠️ Couldn't deliver the image attachment."},
        ])

    async def test_a_failed_image_batch_is_reported_once(self):
        pngs = [self.cached("images", f"{i}.png", PNG) for i in range(2)]
        self.transport.ando.refuse_put = 415
        await self.turn(f"Charts:\nMEDIA:{pngs[0]}\nMEDIA:{pngs[1]}")
        self.assertEqual([r.get("markdown_content") for r in self.replies()],
                         ["Charts:", "⚠️ Couldn't deliver 2 image attachments."])

    async def test_unsupported_file_gets_hermes_notice_and_the_turn_completes(self):
        voice = self.cached("audio", "note.ogg", b"OggS" + b"\x00" * 24 + b"OpusHead" + b"\x00" * 40)
        await self.turn(f"Voice note:\nMEDIA:{voice}")
        self.assertTrue(self.state.completed("event-1"))
        texts = [r.get("markdown_content") for r in self.replies()]
        self.assertEqual(texts[0], "Voice note:")
        self.assertIn("Couldn't deliver the audio attachment", texts[-1])
        self.assertEqual([n for n, _ in self.transport.calls if n == "upload_attachment"], [])

    async def test_inbound_image_reaches_hermes_as_a_cached_path_vision_can_read(self):
        self.transport.ando.stored["in-1"] = PNG
        self.transport.attachments = [
            {"file_id": "in-1", "filename": "screenshot.png", "content_type": "image/png", "size_bytes": len(PNG)}]
        incoming = await self.turn("Looks fine.")
        from agent.image_routing import build_native_content_parts
        from gateway.platforms.event import MessageType

        self.assertEqual(incoming.message_type, MessageType.PHOTO)
        self.assertEqual(incoming.media_types, ["image/png"])
        self.assertTrue(Path(incoming.media_urls[0]).is_file())
        parts, skipped = build_native_content_parts(incoming.text, incoming.media_urls)
        self.assertEqual(([p["type"] for p in parts], skipped), (["text", "image_url"], []))

    async def test_inbound_attachment_only_document_has_no_placeholder_text(self):
        self.transport.ando.stored["in-1"] = PDF
        self.transport.attachments = [
            {"file_id": "in-1", "filename": "brief.pdf", "content_type": "application/pdf", "size_bytes": len(PDF)}]
        self.transport.content = None
        incoming = await self.turn("Read it.")
        from gateway.platforms.event import MessageType

        self.assertEqual((incoming.text, incoming.message_type), ("", MessageType.DOCUMENT))
        self.assertEqual(Path(incoming.media_urls[0]).read_bytes(), PDF)

    async def test_inbound_file_that_cannot_be_read_is_named_in_the_text(self):
        self.transport.attachments = [{"file_id": "gone", "filename": "lost.png", "content_type": "image/png"}]
        self.transport.ando.expired_urls.update({f"https://r2.example/gone?sig={n}" for n in range(1, 5)})
        incoming = await self.turn("ok")
        self.assertEqual(incoming.media_urls, [])
        self.assertEqual(incoming.text,
                         "Hello\n[Ando attachment not available: lost.png (could not be downloaded)]")


if __name__ == "__main__":
    unittest.main()
