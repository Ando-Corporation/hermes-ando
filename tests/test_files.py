import asyncio
import base64
import io
import zipfile
from contextlib import asynccontextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin import files
from plugin.files import AttachmentFailed
from plugin.media import LocalFile, UnsupportedFile, failure_text, sniff
from plugin.state import DeliveryState
from plugin.transport import ToolError

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)
HAS_HTTPX = importlib.util.find_spec("httpx") is not None
PDF = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"


class Response:
    def __init__(self, status_code, body=None, chunks=()):
        self.status_code = status_code
        self.body = body
        self.chunks = chunks

    def json(self):
        if self.body is None:
            raise ValueError("no json")
        return self.body

    async def aiter_bytes(self):
        for chunk in self.chunks:
            yield chunk


class FakeHttp:
    """Ando's capability endpoints: the PUT stores bytes once, GET serves them."""

    def __init__(self, server):
        self.server = server
        self.requests = []

    async def put(self, url, content, headers):
        self.requests.append(("PUT", url, dict(headers)))
        return self.server.receive_put(url, content, headers)

    @asynccontextmanager
    async def stream(self, method, url):
        self.requests.append((method, url, {}))
        yield self.server.serve(url)


class FakeAndo:
    def __init__(self):
        self.calls = []
        self.uploads = {}  # idempotency key -> {file_id, status, url}
        self.stored = {}  # file_id -> bytes
        self.refuse_put = None
        self.refuse_names = set()
        self.rejected_keys = set()
        self.expired_urls = set()
        self.minted = 0

    async def call(self, name, args):
        self.calls.append((name, dict(args)))
        if name == "upload_attachment":
            key = args["idempotency_key"]
            if key in self.rejected_keys:
                raise ToolError("Ando upload_attachment failed; review the connection in Ando")
            upload = self.uploads.setdefault(
                key, {"file_id": f"file-{len(self.uploads) + 1}", "status": "waiting_for_upload"}
            )
            if upload["status"] == "ready":
                return {"file_id": upload["file_id"], "status": "ready", "upload": None}
            self.minted += 1
            upload["url"] = f"https://site.example/mcp-attachments/mau_{self.minted}"
            upload["content_type"] = args["content_type"]
            upload["filename"] = args["filename"]
            return {
                "file_id": upload["file_id"],
                "status": "waiting_for_upload",
                "upload": {
                    "method": "PUT",
                    "url": upload["url"],
                    "headers": {"Content-Type": args["content_type"]},
                    "expires_at": "2026-10-16T19:00:00Z",
                },
            }
        if name == "get_file":
            self.minted += 1
            url = f"https://r2.example/{args['file_id']}?sig={self.minted}"
            return {"data": {"file_id": args["file_id"], "download": {"method": "GET", "url": url}}}
        raise AssertionError(name)

    def receive_put(self, url, content, headers):
        upload = next(u for u in self.uploads.values() if u.get("url") == url)
        if self.refuse_put or upload["filename"] in self.refuse_names:
            return Response(self.refuse_put or 415, {"error": "content_type_mismatch"})
        if upload["status"] == "ready":
            return Response(410, {"error": "upload_url_used"})
        upload["status"] = "ready"
        self.stored[upload["file_id"]] = content
        return Response(200, {"file_id": upload["file_id"], "status": "ready", "size_bytes": len(content)})

    def serve(self, url):
        if url in self.expired_urls:
            return Response(403)
        file_id = url.split("/")[-1].split("?")[0]
        data = self.stored[file_id]
        return Response(200, chunks=[data[:3], data[3:]])

    def puts(self, http):
        return [r for r in http.requests if r[0] == "PUT"]


class FileTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.state = DeliveryState(self.dir / "state.sqlite3", "workspace", "agent")
        self.ando = FakeAndo()
        self.http = FakeHttp(self.ando)

    def tearDown(self):
        self.state.close()
        self.tmp.cleanup()

    def write(self, name, data):
        path = self.dir / name
        path.write_bytes(data)
        return path


class SniffTests(unittest.TestCase):
    def test_declares_what_the_bytes_are(self):
        cases = {
            PNG: "image/png",
            b"\xff\xd8\xff\xe0\x00\x10JFIF": "image/jpeg",
            b"GIF89a\x01\x00": "image/gif",
            b"RIFF\x00\x00\x00\x00WEBPVP8 ": "image/webp",
            b"RIFF\x00\x00\x00\x00WAVEfmt ": "audio/wav",
            PDF: "application/pdf",
            b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00": "video/mp4",
            b"\x00\x00\x00\x14ftypqt  \x00\x00\x00\x00": "video/quicktime",
            b"\x1a\x45\xdf\xa3\x9fB\x86\x81\x01B\x82\x84webm": "video/webm",
            b"ID3\x04\x00\x00\x00\x00\x00\x00": "audio/mpeg",
            b"\xff\xfb\x90\x64\x00": "audio/mpeg",
            b"OggS\x00\x02" + b"\x00" * 22 + b"\x01vorbis": "audio/ogg",
        }
        for data, expected in cases.items():
            self.assertEqual(sniff(data, "x"), expected, data[:12])

    def test_refuses_what_ando_rejects(self):
        for data in [
            b"OggS\x00\x02" + b"\x00" * 22 + b"OpusHead",  # sniffs as audio/ogg; codecs=opus
            b"\x00\x00\x00\x20ftypM4A \x00\x00\x00\x00",  # audio/x-m4a
            b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00",
            b"\x1a\x45\xdf\xa3\x9fB\x86\x81\x01B\x82\x88matroska",
            b"<?xml version='1.0'?><svg/>",
            b"\x00\x01binary",
            b"\xc3\x28 invalid utf-8",
            b"",
        ]:
            self.assertIsNone(sniff(data, "x.bin"), data[:12])

    def test_text_type_follows_extension_and_zip_follows_office_suffix(self):
        self.assertEqual(sniff(b"a,b\n1,2\n", "data.csv"), "text/csv")
        self.assertEqual(sniff(b"# Notes", "notes.md"), "text/markdown")
        self.assertEqual(sniff(b'{"a":1}', "a.json"), "application/json")
        self.assertEqual(sniff(b"print('hi')\n", "tool.py"), "text/plain")
        self.assertEqual(sniff(b"PK\x03\x04rest", "bundle.zip"), "application/zip")

    def test_zip_type_comes_from_its_contents_not_its_name(self):
        def archive(entries):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as z:
                for name, body in entries.items():
                    z.writestr(name, body)
            return buffer.getvalue()

        def office(main):
            return archive({"[Content_Types].xml": (
                '<Types><Override PartName="/word/document.xml" '
                f'ContentType="application/vnd.{main}.main+xml"/></Types>'), "word/document.xml": "<w/>"})
        docx = office("openxmlformats-officedocument.wordprocessingml.document")
        self.assertEqual(sniff(docx, "report.zip"),
                         "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        self.assertEqual(sniff(office("openxmlformats-officedocument.spreadsheetml.sheet"), "data"),
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.assertIsNone(sniff(office("ms-word.document.macroEnabled"), "macro.docx"))
        self.assertEqual(sniff(archive({"notes.txt": "hi"}), "deck.pptx"), "application/zip")
        self.assertIsNone(sniff(archive({"META-INF/MANIFEST.MF": "x"}), "tool.zip"))
        self.assertIsNone(sniff(archive({"mimetype": "application/epub+zip"}), "book.zip"))


class LocalFileTests(FileTestCase):
    def test_freezes_size_hash_and_a_safe_name(self):
        path = self.write("chart.png", PNG)
        file = LocalFile.read(path, "../../etc/\x07report.png")
        self.assertEqual(file.filename, "report.png")
        self.assertEqual((file.content_type, file.size), ("image/png", len(PNG)))
        self.assertEqual(file.sha256, hashlib.sha256(PNG).hexdigest())
        self.assertEqual(LocalFile.from_json(file.to_json()), file)

    def test_refuses_unsupported_empty_and_missing_files(self):
        for path in [self.write("voice.ogg", b"OggS" + b"\x00" * 24 + b"OpusHead"),
                     self.write("empty.txt", b""), self.dir / "missing.pdf"]:
            with self.assertRaises(UnsupportedFile):
                LocalFile.read(path)

    def test_changed_file_is_not_the_frozen_file(self):
        path = self.write("doc.pdf", PDF)
        file = LocalFile.read(path)
        self.assertEqual(file.current_bytes(), PDF)
        path.write_bytes(PDF + b"edited")
        self.assertIsNone(file.current_bytes())
        path.unlink()
        self.assertIsNone(file.current_bytes())


class OutgoingTests(FileTestCase):
    def files(self):
        return [LocalFile.read(self.write("chart.png", PNG)), LocalFile.read(self.write("doc.pdf", PDF))]

    async def test_live_reply_uploads_each_file_once_and_returns_its_ids(self):
        arguments = await files.upload_reply_files(
            self.ando, self.state, "event-1", 1, self.files(), "Here you go", self.http)
        self.assertEqual(arguments, {"markdown_content": "Here you go", "file_ids": ["file-1", "file-2"]})
        self.assertEqual(self.ando.stored, {"file-1": PNG, "file-2": PDF})
        puts = self.ando.puts(self.http)
        self.assertEqual(len(puts), 2)
        self.assertEqual(puts[0][2], {"Content-Type": "image/png"})
        keys = [args["idempotency_key"] for name, args in self.ando.calls]
        frozen = self.files()
        self.assertEqual(keys, [self.state.upload_key("event-1", 1, 0, frozen[0]),
                                self.state.upload_key("event-1", 1, 1, frozen[1])])
        self.assertEqual(self.state.pending_uploads("event-1"), [
            (1, files.intent(self.files(), "Here you go"), arguments)])

    async def test_attachment_only_reply_carries_no_text(self):
        arguments = await files.upload_reply_files(
            self.ando, self.state, "event-1", 0, self.files()[:1], None, self.http)
        self.assertEqual(arguments, {"file_ids": ["file-1"]})

    async def test_refused_upload_discards_the_intent_so_hermes_can_report_it(self):
        self.ando.refuse_put = 415
        with self.assertRaisesRegex(AttachmentFailed, "HTTP 415"):
            await files.upload_reply_files(self.ando, self.state, "event-1", 2, self.files(), None, self.http)
        self.assertEqual(self.state.pending_uploads("event-1"), [])

    async def test_cancellation_keeps_the_intent_so_a_restart_finishes_the_reply(self):
        original = self.http.put

        async def gateway_stops(*args, **kwargs):
            raise asyncio.CancelledError
        self.http.put = gateway_stops
        with self.assertRaises(asyncio.CancelledError):
            await files.upload_reply_files(self.ando, self.state, "event-1", 0, self.files(), None, self.http)
        self.assertEqual([o for o, _, _ in self.state.pending_uploads("event-1")], [0])
        self.http.put = original
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"file_ids": ["file-1", "file-2"]})])

    async def test_resume_with_uploads_turned_off_posts_notices_and_uploads_nothing(self):
        frozen = self.files()
        self.state.prepare_upload("event-1", 0, files.intent(frozen, "Report"))
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http, can_upload=False)
        self.assertEqual(resumed, [(0, {"markdown_content": (
            "Report\n⚠️ Couldn't deliver the image attachment.\n⚠️ Couldn't deliver the file attachment.")})])
        self.assertEqual(self.ando.calls, [])

    async def test_uploads_turned_off_after_files_landed_still_post_no_files(self):
        frozen = self.files()[:1]
        self.state.prepare_upload("event-1", 0, files.intent(frozen, None))
        await files.resume_reply_files(self.ando, self.state, "event-1", self.http)  # landed, then a crash
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http, can_upload=False)
        self.assertEqual(resumed, [(0, {"markdown_content": "⚠️ Couldn't deliver the image attachment."})])

    async def test_a_second_crash_during_recovery_keeps_the_failure_notices(self):
        frozen = self.files()
        self.state.prepare_upload("event-1", 0, files.intent(frozen, "Report"))
        Path(frozen[1].path).unlink()
        first = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        # The process dies before the reply is prepared; the next recovery reads it back.
        second = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(second, first)
        self.assertIn("Couldn't deliver the file attachment", second[0][1]["markdown_content"])
        self.assertEqual(len(self.ando.puts(self.http)), 1)

    async def test_a_later_batch_at_the_same_ordinal_never_gets_the_failed_batch_files(self):
        chart, brief = self.files()
        self.ando.refuse_names = {"doc.pdf"}
        with self.assertRaises(AttachmentFailed):  # chart uploaded, then the batch failed
            await files.upload_reply_files(self.ando, self.state, "event-1", 0, [chart, brief], None, self.http)
        other = LocalFile.read(self.write("other.png", PNG + b"\x00"), "chart.png")
        arguments = await files.upload_reply_files(self.ando, self.state, "event-1", 0, [other], None, self.http)
        self.assertEqual(self.ando.stored[arguments["file_ids"][0]], PNG + b"\x00")

    async def test_the_same_bytes_under_another_name_get_their_own_upload(self):
        chart, brief = self.files()
        self.ando.refuse_names = {"doc.pdf"}
        with self.assertRaises(AttachmentFailed):  # chart.png uploaded, then the batch failed
            await files.upload_reply_files(self.ando, self.state, "event-1", 0, [chart, brief], None, self.http)
        renamed = LocalFile.read(Path(chart.path), "revenue.png")
        arguments = await files.upload_reply_files(self.ando, self.state, "event-1", 0, [renamed], None, self.http)
        upload = next(u for u in self.ando.uploads.values() if u["file_id"] == arguments["file_ids"][0])
        self.assertEqual(upload["filename"], "revenue.png")

    async def test_recovery_retries_a_refused_upload_once_with_a_fresh_key(self):
        frozen = self.files()[:1]
        self.state.prepare_upload("event-1", 0, files.intent(frozen, None))
        self.ando.rejected_keys = {self.state.upload_key("event-1", 0, 0, frozen[0])}  # expired
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"file_ids": ["file-1"]})])
        self.assertEqual(self.ando.stored["file-1"], PNG)

    async def test_recovery_turns_a_permanent_refusal_into_text_but_retries_lost_connections(self):
        frozen = self.files()[:1]
        self.state.prepare_upload("event-1", 0, files.intent(frozen, "Chart"))
        original = self.ando.call

        async def offline(name, args):
            raise OSError("connection reset")
        self.ando.call = offline
        with self.assertRaises(OSError):
            await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(self.state.pending_uploads("event-1")[0][2], None)  # nothing frozen yet
        self.ando.call = original
        self.ando.rejected_keys = {self.state.upload_key("event-1", 0, 0, frozen[0], n) for n in (0, 1)}
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"markdown_content": "Chart\n⚠️ Couldn't deliver the image attachment."})])

    async def test_resume_after_crash_between_put_and_reply_does_not_upload_again(self):
        frozen = self.files()
        self.state.prepare_upload("event-1", 0, files.intent(frozen, "cap"))
        for index, file in enumerate(frozen):  # the PUTs landed, then the process died
            await files.upload(self.ando, file, file.current_bytes(),
                               self.state.upload_key("event-1", 0, index, file), self.http)
        before = len(self.ando.puts(self.http))
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"markdown_content": "cap", "file_ids": ["file-1", "file-2"]})])
        self.assertEqual(len(self.ando.puts(self.http)), before)

    async def test_resume_after_crash_before_put_uploads_with_the_same_keys(self):
        frozen = self.files()
        self.state.prepare_upload("event-1", 0, files.intent(frozen, None))
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"file_ids": ["file-1", "file-2"]})])
        self.assertEqual(set(self.ando.uploads), {
            self.state.upload_key("event-1", 0, 0, frozen[0]),
            self.state.upload_key("event-1", 0, 1, frozen[1])})

    async def test_resume_turns_a_vanished_file_into_hermes_failure_text(self):
        frozen = self.files()
        self.state.prepare_upload("event-1", 0, files.intent(frozen, "Report"))
        Path(frozen[1].path).unlink()
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {
            "markdown_content": "Report\n⚠️ Couldn't deliver the file attachment.", "file_ids": ["file-1"]})])

    async def test_resume_with_nothing_left_still_posts_text(self):
        frozen = self.files()[:1]
        self.state.prepare_upload("event-1", 0, files.intent(frozen, None))
        Path(frozen[0].path).unlink()
        resumed = await files.resume_reply_files(self.ando, self.state, "event-1", self.http)
        self.assertEqual(resumed, [(0, {"markdown_content": "⚠️ Couldn't deliver the image attachment."})])

    async def test_capability_urls_get_no_credentials_and_must_be_https(self):
        await files.upload_reply_files(self.ando, self.state, "event-1", 0, self.files()[:1], None, self.http)
        for _, _, headers in self.http.requests:
            self.assertFalse({h.lower() for h in headers} & {"authorization", "x-api-key"})
        for url in ["http://site.example/mcp-attachments/x", "ftp://x", "data:image/png;base64,AA", None]:
            with self.assertRaises(AttachmentFailed):
                await files.put_bytes({"url": url, "headers": {}}, PNG, self.http)
        self.assertEqual(
            (await files.put_bytes({"url": "http://127.0.0.1:3211/x", "headers": {}}, PNG, LocalPut())),
            {"ok": True})

    def test_too_many_or_unsupported_files_fail_before_any_upload(self):
        with self.assertRaises(AttachmentFailed):
            files.local_files([(self.write(f"{i}.png", PNG), None) for i in range(11)])
        with self.assertRaisesRegex(AttachmentFailed, "does not accept"):
            files.local_files([(self.write("voice.m4a", b"\x00\x00\x00\x20ftypM4A \x00\x00"), None)])

    @unittest.skipUnless(HAS_HTTPX, "httpx not installed")
    async def test_default_client_carries_no_ando_credential(self):
        client = files._client()
        try:
            self.assertNotIn("authorization", {k.lower() for k in client.headers})
            self.assertFalse(client.follow_redirects)
        finally:
            await client.aclose()


class LocalPut:
    async def put(self, url, content, headers):
        return Response(200, {"ok": True})


class StateTests(FileTestCase):
    def test_reply_keys_are_unchanged_for_outboxes_written_by_older_releases(self):
        args, _ = self.state.prepare_reply("event-1", 0, "send_message", {"markdown_content": "hi"})
        identity = json.dumps(["workspace", "agent"])
        expected = hashlib.sha256(f"{identity}:event-1:0".encode()).hexdigest()
        self.assertEqual(args["idempotency_key"], f"hermes-ando:{expected}")

    def test_an_intent_cannot_change_and_a_sent_reply_keeps_its_intent(self):
        self.state.prepare_upload("event-1", 0, "a")
        with self.assertRaises(ValueError):
            self.state.prepare_upload("event-1", 0, "b")
        self.state.prepare_reply("event-1", 0, "send_message", {"file_ids": ["f"]})
        self.assertEqual(self.state.pending_uploads("event-1"), [])
        self.state.discard_upload("event-1", 0)
        self.state.prepare_upload("event-1", 0, "a")  # still frozen, not deleted

    def test_existing_databases_gain_the_uploads_table(self):
        path = self.dir / "old.sqlite3"
        import sqlite3

        old = sqlite3.connect(path)
        old.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE events (id TEXT PRIMARY KEY, completed INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE replies (event_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                tool TEXT NOT NULL, arguments TEXT NOT NULL, message_id TEXT,
                PRIMARY KEY(event_id, ordinal));
        """)
        old.close()
        state = DeliveryState(path, "workspace", "agent")
        try:
            self.assertEqual(state.pending_uploads("event-1"), [])
        finally:
            state.close()


class IncomingTests(FileTestCase):
    def message(self, *items):
        return {"id": "m", "content": None, "attachments": list(items)}

    def stored(self, file_id, data, name, content_type):
        self.ando.stored[file_id] = data
        return {"file_id": file_id, "filename": name, "content_type": content_type, "size_bytes": len(data)}

    async def test_downloads_every_attachment_at_dispatch(self):
        message = self.message(self.stored("a", PNG, "photo.png", "image/png"),
                               self.stored("b", PDF, "brief.pdf", "application/pdf"))
        fetched, missing = await files.fetch_attachments(self.ando, message, client=self.http)
        self.assertEqual(fetched, [(PNG, "photo.png", "image/png"), (PDF, "brief.pdf", "application/pdf")])
        self.assertEqual(missing, [])
        self.assertEqual([name for name, _ in self.ando.calls], ["get_file", "get_file"])

    async def test_an_expired_url_gets_one_fresh_get_file(self):
        message = self.message(self.stored("a", PNG, "photo.png", "image/png"))
        original = self.ando.call

        async def first_url_expired(name, args):
            result = await original(name, args)
            if len(self.ando.calls) == 1:
                self.ando.expired_urls.add(result["data"]["download"]["url"])
            return result
        self.ando.call = first_url_expired
        fetched, missing = await files.fetch_attachments(self.ando, message, client=self.http)
        self.assertEqual(len(fetched), 1)
        self.assertEqual(len(self.ando.calls), 2)

    async def test_unreadable_files_become_visible_notes(self):
        items = [self.stored(f"f{i}", PNG, f"{i}.png", "image/png") for i in range(11)]
        items[0] = {**items[0], "size_bytes": 30_000_000}
        self.ando.expired_urls.update({f"https://r2.example/f1?sig={n}" for n in range(1, 10)})
        fetched, missing = await files.fetch_attachments(self.ando, self.message(*items), client=self.http)
        self.assertEqual(len(fetched), 8)
        self.assertEqual(missing[0], ("0.png", "larger than the inbound limit"))
        self.assertEqual(missing[1], ("1.png", "could not be downloaded"))
        self.assertEqual(missing[2], ("10.png", "only the first 10 attachments are read"))
        self.assertEqual(files.missing_note(missing[:1]),
                         "[Ando attachment not available: 0.png (larger than the inbound limit)]")

    async def test_disabled_file_reading_reads_nothing(self):
        message = self.message(self.stored("a", PNG, "photo.png", "image/png"))
        fetched, missing = await files.fetch_attachments(self.ando, message, can_read=False, client=self.http)
        self.assertEqual((fetched, self.ando.calls), ([], []))
        self.assertEqual(missing, [("photo.png", "file reading is disabled in Settings > Members")])

    async def test_download_stops_at_the_limit(self):
        message = self.message({**self.stored("a", PDF, "big.pdf", "application/pdf"), "size_bytes": 1})
        fetched, missing = await files.fetch_attachments(self.ando, message, limit=10, client=self.http)
        self.assertEqual((fetched, missing), ([], [("big.pdf", "could not be downloaded")]))


class FailureTextTests(unittest.TestCase):
    def test_matches_hermes_wording(self):
        self.assertEqual(failure_text("application/pdf"), "⚠️ Couldn't deliver the file attachment.")
        self.assertEqual(failure_text("video/mp4"), "⚠️ Couldn't deliver the video attachment.")


if __name__ == "__main__":
    unittest.main()
