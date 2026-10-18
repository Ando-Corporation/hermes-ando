"""Attachments in both directions for one identity-pinned Ando turn.

Outgoing files are frozen in the outbox before any network call and uploaded with
idempotency keys derived from the reply's ordinal, so a replay after a crash
neither re-uploads finished bytes nor posts a second message. Incoming files are
downloaded at dispatch: a file on someone else's message gets a 60-second URL.

Upload and download URLs are single-use capabilities on another host. They get
no Ando credential, never follow redirects, and are never logged.
"""

import json
import logging
from urllib.parse import urlsplit

from .media import LocalFile, UnsupportedFile, failure_text
from .transport import ToolError

logger = logging.getLogger(__name__)

MAX_FILES = 10
MAX_INBOUND_BYTES = 20_000_000


class AttachmentFailed(RuntimeError):
    """The attachment was not delivered; Hermes posts its own notice."""


def _require_capability_url(url):
    parts = urlsplit(url if isinstance(url, str) else "")
    local = parts.hostname in {"localhost", "127.0.0.1", "::1"}
    if not parts.netloc or not (
        parts.scheme == "https" or (parts.scheme == "http" and local)
    ):
        raise AttachmentFailed("Ando returned an unusable file URL")


def _client():
    import httpx

    # Deliberately not the MCP client: no Authorization or x-api-key header.
    return httpx.AsyncClient(
        follow_redirects=False, timeout=httpx.Timeout(30.0, read=120.0)
    )


async def put_bytes(upload, data, client=None):
    _require_capability_url(upload.get("url"))
    headers = dict(upload.get("headers") or {})
    owned = client is None
    client = client or _client()
    try:
        response = await client.put(upload["url"], content=data, headers=headers)
    finally:
        if owned:
            await client.aclose()
    if response.status_code != 200:
        raise AttachmentFailed(f"Ando refused the file (HTTP {response.status_code})")
    try:
        return response.json()
    except ValueError:
        raise AttachmentFailed("Ando returned an unreadable upload result") from None


async def fetch_bytes(url, limit, client=None):
    _require_capability_url(url)
    owned = client is None
    client = client or _client()
    try:
        async with client.stream("GET", url) as response:
            if response.status_code != 200:
                raise AttachmentFailed(f"download failed (HTTP {response.status_code})")
            chunks, total = [], 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > limit:
                    raise AttachmentFailed("file is larger than the inbound limit")
                chunks.append(chunk)
            return b"".join(chunks)
    finally:
        if owned:
            await client.aclose()


async def upload(transport, file, data, idempotency_key, client=None):
    """One file to a ready Ando file_id. Safe to repeat with the same key."""
    prepared = await transport.call(
        "upload_attachment",
        {
            "filename": file.filename,
            "content_type": file.content_type,
            "size_bytes": file.size,
            "idempotency_key": idempotency_key,
        },
    )
    file_id = prepared.get("file_id")
    if not isinstance(file_id, str) or not file_id:
        raise AttachmentFailed("Ando did not prepare the upload")
    if prepared.get("status") == "ready" and prepared.get("upload") is None:
        return file_id
    result = await put_bytes(prepared.get("upload") or {}, data, client)
    if result.get("status") != "ready" or result.get("file_id") != file_id:
        raise AttachmentFailed("Ando did not confirm the uploaded file")
    return file_id


def intent(files, caption):
    return json.dumps(
        {"files": [f.to_json() for f in files], "caption": caption or None},
        sort_keys=True,
        separators=(",", ":"),
    )


async def upload_reply_files(transport, state, event_id, ordinal, files, caption, client=None):
    """Freeze, then upload, a live reply's files. Returns the reply arguments to send.

    When the upload fails the frozen intent is discarded and nothing is posted,
    so Hermes's own failure notice can take this ordinal instead. Cancellation
    (a gateway restart) keeps the intent: the next delivery finishes the reply.
    """
    state.prepare_upload(event_id, ordinal, intent(files, caption))
    try:
        file_ids = []
        for index, file in enumerate(files):
            data = file.current_bytes()
            if data is None:
                raise AttachmentFailed("The file changed before it was uploaded")
            key = state.upload_key(event_id, ordinal, index, file)
            file_ids.append(await upload(transport, file, data, key, client))
    except Exception:
        state.discard_upload(event_id, ordinal)
        raise
    reply = _reply_arguments(caption, file_ids)
    state.uploaded(event_id, ordinal, reply)
    return reply


async def resume_reply_files(transport, state, event_id, client=None, *, can_upload=True):
    """After a crash: finish each frozen attachment reply that was never prepared.

    Returns [(ordinal, arguments)] for the caller to freeze and send. Files that
    vanished or changed since, or that uploads now turned off in Settings >
    Members would carry, become Hermes's failure text, never a silent drop.
    """
    resumed = []
    for ordinal, frozen, reply in state.pending_uploads(event_id):
        value = json.loads(frozen)
        caption = value.get("caption")
        if reply is not None and (can_upload or not reply.get("file_ids")):
            resumed.append((ordinal, reply))
            continue
        file_ids, notices = [], []
        for index, raw in enumerate(value["files"]):
            file = LocalFile.from_json(raw)
            data = file.current_bytes() if can_upload else None
            if data is None:
                notices.append(failure_text(file.content_type))
                continue
            file_id = await _upload_on_recovery(transport, state, event_id, ordinal, index, file, data, client)
            if file_id is None:
                notices.append(failure_text(file.content_type))
            else:
                file_ids.append(file_id)
        text = "\n".join(part for part in [caption, *notices] if part)
        reply = _reply_arguments(text, file_ids)
        state.uploaded(event_id, ordinal, reply)
        resumed.append((ordinal, reply))
    return resumed


async def _upload_on_recovery(transport, state, event_id, ordinal, index, file, data, client):
    """A definite refusal gets one fresh, still deterministic key (an upload left
    unfinished while the gateway was down expires), then becomes failure text.
    A lost connection raises, so the next delivery tries again."""
    for attempt in (0, 1):
        key = state.upload_key(event_id, ordinal, index, file, attempt)
        try:
            return await upload(transport, file, data, key, client)
        except (AttachmentFailed, ToolError):
            continue
    return None


def _reply_arguments(caption, file_ids):
    arguments = {}
    if caption:
        arguments["markdown_content"] = caption
    if file_ids:
        arguments["file_ids"] = list(file_ids)
    if not arguments:
        arguments["markdown_content"] = failure_text("")
    return arguments


def local_files(paths):
    """[(path, filename)] -> LocalFiles, or raise for anything Ando would refuse."""
    if not paths:
        raise AttachmentFailed("No attachment to send")
    if len(paths) > MAX_FILES:
        raise AttachmentFailed(f"At most {MAX_FILES} files per Ando message")
    try:
        return [LocalFile.read(path, filename) for path, filename in paths]
    except UnsupportedFile as exc:
        raise AttachmentFailed(str(exc)) from None


async def fetch_attachments(transport, message, *, can_read=True, limit=MAX_INBOUND_BYTES, client=None):
    """Download a message's attachments now, while their short-lived URLs are valid.

    Returns (fetched, missing): fetched is [(bytes, filename, content_type)],
    missing is [(filename, reason)] for the turn's text so nothing is hidden.
    """
    attachments = [
        item for item in message.get("attachments") or [] if isinstance(item, dict)
    ]
    fetched, missing = [], []
    for position, item in enumerate(attachments):
        name = item.get("filename") or "attachment"
        if position >= MAX_FILES:
            missing.append((name, f"only the first {MAX_FILES} attachments are read"))
            continue
        if not can_read:
            missing.append((name, "file reading is disabled in Settings > Members"))
            continue
        if isinstance(item.get("size_bytes"), int) and item["size_bytes"] > limit:
            missing.append((name, "larger than the inbound limit"))
            continue
        try:
            fetched.append((await _download(transport, item, limit, client), name,
                            item.get("content_type") or ""))
        except Exception as exc:  # noqa: BLE001 - every failure becomes a visible note
            logger.warning("Ando attachment %s could not be read (%s)", item.get("file_id"),
                           type(exc).__name__)
            missing.append((name, "could not be downloaded"))
    return fetched, missing


async def _download(transport, item, limit, client):
    last = None
    for _ in range(2):
        # Each get_file signs a fresh URL; a slow first try gets one more.
        result = await transport.call("get_file", {"file_id": item["file_id"]})
        download = (result.get("data") or result).get("download") or {}
        try:
            return await fetch_bytes(download.get("url"), limit, client)
        except AttachmentFailed as exc:
            last = exc
    raise last


def missing_note(missing):
    return "\n".join(f"[Ando attachment not available: {name} ({reason})]" for name, reason in missing)
