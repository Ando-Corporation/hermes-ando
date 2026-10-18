"""MessageDelivery with attachments: one durable reply per file batch, exactly once."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.delivery import MessageDelivery
from plugin.files import AttachmentFailed
from plugin.protocol import SetupRequired
from plugin.state import DeliveryState
from test_delivery import FakeTransport, event
from test_files import PDF, PNG, FakeAndo, FakeHttp


class FilesTransport(FakeTransport):
    """The adapter's tools plus Ando's file tools, sharing one fake server."""

    def __init__(self):
        super().__init__()
        self.ando = FakeAndo()
        self.attachments = []
        self.content = "Hello"
        self.sends = 0

    async def call(self, name, args):
        if name in {"upload_attachment", "get_file"}:
            self.calls.append((name, dict(args)))
            return await self.ando.call(name, args)
        if name in {"send_message", "reply_to_message"}:
            self.sends += 1
        result = await super().call(name, args)
        if name == "get_message":
            result["data"]["attachments"] = self.attachments
            result["data"]["content"] = self.content
        return result


class ClosingHttp(FakeHttp):
    async def aclose(self):
        pass


class DeliveryFilesTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.path = self.dir / "state.sqlite3"
        self.state = DeliveryState(self.path, "workspace", "agent")
        self.transport = FilesTransport()
        self.http = ClosingHttp(self.transport.ando)
        self.client = patch("plugin.files._client", lambda: self.http)
        self.client.start()
        self.png = self.dir / "chart.png"
        self.png.write_bytes(PNG)
        self.pdf = self.dir / "brief.pdf"
        self.pdf.write_bytes(PDF)

    def tearDown(self):
        self.client.stop()
        self.state.close()
        self.tmp.cleanup()

    def delivery(self, dispatch):
        return MessageDelivery(self.transport, self.state, "workspace", "agent", ["human"], dispatch)

    def replies(self):
        return [(o, tool, {k: v for k, v in args.items() if k != "idempotency_key"}, mid)
                for o, tool, args, mid in self.state.replies("event-1")]

    async def test_text_then_files_become_ordered_durable_replies(self):
        async def dispatch(ref, message, disabled):
            chat = delivery.chat_id(ref)
            await delivery.send(chat, "Here is the report.")
            await delivery.send_files(chat, [(self.pdf, "Q3 brief.pdf")], None)
            await delivery.send_files(chat, [(self.png, None), (self.png, "copy.png")], "Charts")
        delivery = self.delivery(dispatch)
        await delivery.handle(event())
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies(), [
            (0, "send_message", {"conversation_id": "conversation", "markdown_content": "Here is the report."}, "reply-1"),
            (1, "send_message", {"conversation_id": "conversation", "file_ids": ["file-1"]}, "reply-1"),
            (2, "send_message", {"conversation_id": "conversation", "file_ids": ["file-2", "file-3"],
                                 "markdown_content": "Charts"}, "reply-1"),
        ])
        uploads = [args for name, args in self.transport.calls if name == "upload_attachment"]
        self.assertEqual([u["filename"] for u in uploads], ["Q3 brief.pdf", "chart.png", "copy.png"])

    async def test_thread_replies_carry_files_to_the_thread(self):
        async def dispatch(ref, message, disabled):
            await delivery.send_files(delivery.chat_id(ref), [(self.png, None)])
        delivery = self.delivery(dispatch)
        await delivery.handle(event(thread="root"))
        self.assertEqual(self.replies()[0][1:3], ("reply_to_message", {"message_id": "root", "file_ids": ["file-1"]}))

    async def test_failed_upload_lets_hermes_notice_take_the_slot(self):
        async def dispatch(ref, message, disabled):
            chat = delivery.chat_id(ref)
            self.transport.ando.refuse_put = 415
            with self.assertRaises(AttachmentFailed):
                await delivery.send_files(chat, [(self.png, None)])
            await delivery.send(chat, "⚠️ Couldn't deliver the image attachment.")
        delivery = self.delivery(dispatch)
        await delivery.handle(event())
        self.assertEqual([r[2].get("markdown_content") for r in self.replies()],
                         ["⚠️ Couldn't deliver the image attachment."])
        self.assertTrue(self.state.completed("event-1"))

    async def test_crash_mid_upload_resumes_without_regenerating_or_double_posting(self):
        async def crashes_after_put(ref, message, disabled):
            chat = delivery.chat_id(ref)
            await delivery.send(chat, "Report attached.")
            original = self.state.uploaded

            def die(*args):
                raise KeyboardInterrupt  # the process dies after the bytes landed
            self.state.uploaded = die
            try:
                await delivery.send_files(chat, [(self.pdf, None)])
            finally:
                self.state.uploaded = original
        delivery = self.delivery(crashes_after_put)
        with self.assertRaises(KeyboardInterrupt):
            await delivery.handle(event())
        self.assertEqual([o for o, _, _ in self.state.pending_uploads("event-1")], [1])
        puts_before = len([r for r in self.http.requests if r[0] == "PUT"])
        sends_before = self.transport.sends

        regenerated = []

        async def must_not_run(*args):
            regenerated.append(1)
        await self.delivery(must_not_run).handle(event())
        self.assertEqual(regenerated, [])
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(len([r for r in self.http.requests if r[0] == "PUT"]), puts_before)
        self.assertEqual(self.transport.sends - sends_before, 1)  # only the attachment reply
        self.assertEqual(self.replies()[1][2], {"conversation_id": "conversation", "file_ids": ["file-1"]})

    async def test_media_only_crash_before_any_reply_resumes_instead_of_needing_recovery(self):
        self.state.begin("event-1")
        self.state.prepare_upload("event-1", 0, _intent_for(self.png))
        await self.delivery(None).handle(event())
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies()[0][2], {"conversation_id": "conversation", "file_ids": ["file-1"]})

    async def test_recovery_honors_uploads_turned_off_after_the_crash(self):
        self.state.begin("event-1")
        self.state.prepare_upload("event-1", 0, _intent_for(self.png))
        self.transport.preferences = {
            "disabled_tool_ids": ["t-upload"],
            "tools": [{"id": "t-upload", "name": "upload_attachment", "description": "", "enabled": False}],
        }
        await self.delivery(None).handle(event())
        self.assertTrue(self.state.completed("event-1"))
        self.assertEqual(self.replies()[0][2], {
            "conversation_id": "conversation", "markdown_content": "⚠️ Couldn't deliver the image attachment."})
        self.assertEqual([n for n, _ in self.transport.calls if n == "upload_attachment"], [])

    async def test_disabled_uploads_fail_the_file_not_the_turn(self):
        self.transport.preferences = {
            "disabled_tool_ids": ["t-upload", "t-get-file"],
            "tools": [
                {"id": "t-upload", "name": "upload_attachment", "description": "", "enabled": False},
                {"id": "t-get-file", "name": "get_file", "description": "", "enabled": False},
            ],
        }
        self.transport.attachments = [{"file_id": "x", "filename": "in.png", "content_type": "image/png"}]
        seen = []

        async def dispatch(ref, message, disabled):
            seen.append(await delivery.fetch_attachments(message))
            with self.assertRaisesRegex(AttachmentFailed, "disabled"):
                await delivery.send_files(delivery.chat_id(ref), [(self.png, None)])
            await delivery.send(delivery.chat_id(ref), "text still works")
        delivery = self.delivery(dispatch)
        await delivery.handle(event())
        self.assertEqual(seen, [([], [("in.png", "file reading is disabled in Settings > Members")])])
        self.assertTrue(self.state.completed("event-1"))

    async def test_files_never_go_to_another_conversation(self):
        async def dispatch(ref, message, disabled):
            with self.assertRaises(SetupRequired):
                await delivery.send_files("workspace/agent/other", [(self.png, None)])
            await delivery.send(delivery.chat_id(ref), "ok")
        delivery = self.delivery(dispatch)
        await delivery.handle(event())
        self.assertEqual([n for n, _ in self.transport.calls if n == "upload_attachment"], [])

    async def test_text_reply_arguments_are_unchanged_for_frozen_outboxes(self):
        async def dispatch(ref, message, disabled):
            await delivery.send(delivery.chat_id(ref), "Hello back")
        delivery = self.delivery(dispatch)
        await delivery.handle(event())
        self.assertEqual(sorted(self.state.replies("event-1")[0][2]),
                         ["conversation_id", "idempotency_key", "markdown_content"])


def _intent_for(path):
    from plugin import files
    from plugin.media import LocalFile

    return files.intent([LocalFile.read(path)], None)


if __name__ == "__main__":
    unittest.main()
