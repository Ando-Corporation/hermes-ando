"""Which local files Ando accepts as message attachments, decided from their bytes.

Ando checks uploaded bytes with the `file-type` sniffer and rejects a declared
type that does not match (HTTP 415), so declare what the bytes are rather than
what the extension says. Anything not listed here is refused before any upload.
"""

from dataclasses import asdict, dataclass
import hashlib
import io
from pathlib import Path
import re
import zipfile

MAX_BYTES = 20_000_000

_TEXT_TYPES = {
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".csv": "text/csv",
    ".json": "application/json",
}
_OOXML = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}
# ZIP entries `file-type` reads as another format (xpi, jar, epub/ODF, Keynote, apk).
_NOT_PLAIN_ZIP = re.compile(r"META-INF/mozilla\.rsa|META-INF/MANIFEST\.MF|mimetype|Index/.*\.iwa|classes\d*\.dex")
# Leading bytes `file-type` reports as a binary type; text with one of these is not text.
_NOT_TEXT = (b"<?xml", b"{\\rtf", b"%!PS", b"%PDF", b"\x00")
# ISO base media brands `file-type` reports as something other than video/mp4.
_NOT_MP4 = {b"qt  ", b"M4A ", b"M4B ", b"M4P ", b"heic", b"heix", b"mif1", b"msf1", b"avif"}


class UnsupportedFile(ValueError):
    """Ando would refuse this file; Hermes then posts its own failure notice."""


def sniff(data, filename=""):
    """The content type Ando will accept for these bytes, or None."""
    head = data[:64]
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return "image/webp"
    if head.startswith(b"RIFF") and head[8:12] == b"WAVE":
        return "audio/wav"
    if head.startswith(b"%PDF"):
        return "application/pdf"
    if head[4:8] == b"ftyp":
        brand = head[8:12]
        if brand == b"qt  ":
            return "video/quicktime"
        return None if brand in _NOT_MP4 or brand.startswith(b"3g") else "video/mp4"
    if head.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm" if b"webm" in head else None
    if head.startswith(b"OggS"):
        # Opus sniffs as "audio/ogg; codecs=opus", which Ando does not accept yet.
        return "audio/ogg" if b"\x01vorbis" in head else None
    if head.startswith(b"ID3") or (len(head) > 1 and head[0] == 0xFF and head[1] & 0xE6 == 0xE2):
        return "audio/mpeg"
    if head.startswith(b"PK\x03\x04"):
        return _zip_type(data)
    return _text_type(data, filename)


def _zip_type(data):
    """Office documents by their contents, as Ando's sniffer does, never by name."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
            if any(_NOT_PLAIN_ZIP.fullmatch(name) for name in names):
                return None
            if "[Content_Types].xml" not in names:
                return "application/zip"
            if archive.getinfo("[Content_Types].xml").file_size > 1_000_000:
                return None
            xml = archive.read("[Content_Types].xml").decode("utf-8", "replace")
    except (zipfile.BadZipFile, zipfile.LargeZipFile, OSError, ValueError, KeyError):
        return "application/zip"
    end = xml.find('.main+xml"')
    if end == -1:
        return None
    main = xml[:end]
    main = main[main.rfind('"') + 1:]
    return main if main in _OOXML else None


def _text_type(data, filename):
    if not data or data.lstrip().startswith(_NOT_TEXT) or b"\x00" in data:
        return None
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return _TEXT_TYPES.get(Path(filename).suffix.lower(), "text/plain")


def safe_filename(name, fallback="file"):
    name = re.sub(r"[\x00-\x1f\x7f/\\]", "", Path(name or "").name).strip()
    if name in {"", ".", ".."}:
        name = fallback
    if len(name) > 255:
        suffix = Path(name).suffix[:16]
        name = name[: 255 - len(suffix)] + suffix
    return name


@dataclass(frozen=True)
class LocalFile:
    path: str
    filename: str
    content_type: str
    size: int
    sha256: str

    @classmethod
    def read(cls, path, filename=None):
        """Freeze what will be uploaded; raises UnsupportedFile for anything Ando refuses."""
        source = Path(path)
        try:
            size = source.stat().st_size
        except OSError:
            raise UnsupportedFile("The file is not readable") from None
        if not source.is_file() or size < 1:
            raise UnsupportedFile("The file is empty or not a regular file")
        if size > MAX_BYTES:
            raise UnsupportedFile("The file is larger than 20 MB")
        data = source.read_bytes()
        name = safe_filename(filename or source.name)
        content_type = sniff(data, name)
        if content_type is None:
            raise UnsupportedFile("Ando does not accept this file type")
        return cls(str(source), name, content_type, len(data), hashlib.sha256(data).hexdigest())

    def current_bytes(self):
        """The frozen bytes, or None when the file changed or disappeared since."""
        try:
            data = Path(self.path).read_bytes()
        except OSError:
            return None
        return data if hashlib.sha256(data).hexdigest() == self.sha256 else None

    def to_json(self):
        return asdict(self)

    @classmethod
    def from_json(cls, value):
        return cls(**value)


def kind(content_type):
    """Hermes's own word for the attachment in its failure notice."""
    for prefix, word in (("image/", "image"), ("video/", "video"), ("audio/", "audio")):
        if (content_type or "").startswith(prefix):
            return word
    return "file"


def failure_text(content_type):
    """Hermes's wording (gateway/platforms/base.py _media_failure_text)."""
    return f"⚠️ Couldn't deliver the {kind(content_type)} attachment."


def message_type(media_types):
    """PHOTO/VIDEO/VOICE/DOCUMENT by the first matching prefix, as Hermes's Slack adapter does."""
    from gateway.platforms.event import MessageType

    if not media_types:
        return MessageType.TEXT
    for prefix, value in (
        ("image/", MessageType.PHOTO),
        ("video/", MessageType.VIDEO),
        ("audio/", MessageType.VOICE),
    ):
        if any(m.startswith(prefix) for m in media_types):
            return value
    return MessageType.DOCUMENT

