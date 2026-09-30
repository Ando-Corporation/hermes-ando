"""Durable delivery state, isolated by the verified Ando identity.

Do not advance a cursor merely because Hermes accepted a background task.
Outbox content is frozen before a network write; retries reuse the same key
and bytes rather than asking the model to regenerate a possibly different reply.
"""

import hashlib
import json
import os
import sqlite3
from pathlib import Path


class DeliveryState:
    def __init__(self, path: Path, workspace_id: str, membership_id: str):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        os.chmod(path, 0o600)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, completed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS replies (
                event_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                tool TEXT NOT NULL, arguments TEXT NOT NULL, message_id TEXT,
                PRIMARY KEY(event_id, ordinal)
            );
        """)
        identity = json.dumps([workspace_id, membership_id])
        existing = self.get("identity")
        if existing is not None and existing != identity:
            self.db.close()
            raise ValueError("Ando delivery state belongs to a different agent")
        self.set("identity", identity)

    def close(self):
        self.db.close()

    def get(self, key):
        row = self.db.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set(self, key, value):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, value)
            )

    def completed(self, event_id):
        row = self.db.execute(
            "SELECT completed FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        return bool(row and row[0])

    def complete(self, event_id):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO events VALUES (?, 1)", (event_id,))

    def started(self, event_id):
        return (
            self.db.execute(
                "SELECT 1 FROM events WHERE id = ? AND completed = 0", (event_id,)
            ).fetchone()
            is not None
        )

    def begin(self, event_id):
        with self.db:
            self.db.execute("INSERT INTO events VALUES (?, 0)", (event_id,))

    def retry_after_review(self, event_id):
        if not self.started(event_id) or self.replies(event_id):
            raise ValueError(
                "Only interrupted generation without a prepared reply can be retried"
            )
        with self.db:
            self.db.execute(
                "DELETE FROM events WHERE id = ? AND completed = 0", (event_id,)
            )

    def replies(self, event_id):
        rows = self.db.execute(
            "SELECT ordinal, tool, arguments, message_id FROM replies WHERE event_id = ? ORDER BY ordinal",
            (event_id,),
        ).fetchall()
        return [
            (ordinal, tool, json.loads(arguments), message_id)
            for ordinal, tool, arguments, message_id in rows
        ]

    def prepare_reply(self, event_id, ordinal, tool, arguments):
        key = hashlib.sha256(
            f"{self.get('identity')}:{event_id}:{ordinal}".encode()
        ).hexdigest()
        arguments = {**arguments, "idempotency_key": f"hermes-ando:{key}"}
        serialized = json.dumps(arguments, sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO replies VALUES (?, ?, ?, ?, NULL)",
                (event_id, ordinal, tool, serialized),
            )
        row = self.db.execute(
            "SELECT tool, arguments, message_id FROM replies WHERE event_id = ? AND ordinal = ?",
            (event_id, ordinal),
        ).fetchone()
        if row[0] != tool or row[1] != serialized:
            raise ValueError(
                "A retry attempted to change an already prepared Ando reply"
            )
        return json.loads(row[1]), row[2]

    def sent(self, event_id, ordinal, message_id):
        with self.db:
            self.db.execute(
                "UPDATE replies SET message_id = ? WHERE event_id = ? AND ordinal = ?",
                (message_id, event_id, ordinal),
            )
