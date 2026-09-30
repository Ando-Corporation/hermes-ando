"""Installed CLI preserves credentials and unrelated Hermes configuration."""

import copy
from contextlib import asynccontextmanager, redirect_stdout
import io
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.cli import connect
from plugin.connection import read_connection, private_write
from plugin.protocol import SetupRequired
from test_invitation import VALUE


class Transport:
    fail = False

    def __init__(self, *args, **kwargs):
        pass

    @asynccontextmanager
    async def connected(self):
        if self.fail:
            raise OSError("unavailable")
        yield self

    async def call(self, name, args):
        if name == "get_current_identity":
            return {
                "identity": {
                    "identity_type": "agent",
                    "workspace_id": VALUE["workspace_id"],
                    "workspace_membership_id": VALUE["agent_membership_id"],
                }
            }
        return {
            "workspace": {"workspace_id": VALUE["workspace_id"]},
            "installed_by": {
                "workspace_membership_id": VALUE["connected_by_membership_id"]
            },
        }


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE for installed CLI dependency checks",
)
class CliTests(unittest.IsolatedAsyncioTestCase):
    async def test_import_resume_and_failure_preserve_identity_and_configuration(self):
        import json

        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            config = {
                "model": {"default": "existing"},
                "mcp_servers": {"other": {"url": "https://example.invalid/mcp"}},
            }
            original = copy.deepcopy(config)
            saved = []
            args = SimpleNamespace(invite_stdin=False, credential_stdin=True, name=None)
            output = io.StringIO()
            with (
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(VALUE))),
                redirect_stdout(output),
            ):
                Transport.fail = True
                with self.assertRaises(OSError):
                    await connect(
                        args,
                        home,
                        config,
                        lambda value: saved.append(copy.deepcopy(value)),
                    )
                self.assertEqual(config, original)
                self.assertEqual(saved, [])
                stored = read_connection(home / "ando/connection.json")
                self.assertEqual(stored["api_key"], VALUE["api_key"])
                Transport.fail = False
                args.credential_stdin = False
                await connect(
                    args, home, config, lambda value: saved.append(copy.deepcopy(value))
                )
            self.assertEqual(config["model"], original["model"])
            self.assertEqual(config["mcp_servers"], original["mcp_servers"])
            self.assertEqual(
                config["gateway"]["platforms"]["ando"]["extra"]["membership_id"],
                VALUE["agent_membership_id"],
            )
            self.assertEqual(
                read_connection(home / "ando/connection.json")["receiver_id"],
                stored["receiver_id"],
            )
            self.assertNotIn(VALUE["api_key"], output.getvalue())

    async def test_failed_reconnect_preserves_working_credential(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            path = home / "ando/connection.json"
            previous = dict(VALUE, receiver_id="12345678-1234-4234-8234-123456789012")
            private_write(path, previous)
            replacement = dict(VALUE, api_key="replacement-key")
            args = SimpleNamespace(invite_stdin=False, credential_stdin=True, name=None)
            with (
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(replacement))),
                patch.object(Transport, "fail", True),
            ):
                with self.assertRaises(OSError):
                    await connect(args, home, {}, lambda value: self.fail("saved config"))
            self.assertEqual(read_connection(path), previous)
            pending = path.with_name("connection-pending.json")
            self.assertEqual(read_connection(pending)["api_key"], replacement["api_key"])
            self.assertEqual(pending.stat().st_mode & 0o777, 0o600)
            args.credential_stdin = False
            with (
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(replacement))),
                redirect_stdout(io.StringIO()),
            ):
                await connect(args, home, {}, lambda value: None)
            stored = read_connection(path)
            self.assertFalse(pending.exists())
            self.assertEqual(stored["api_key"], replacement["api_key"])
            self.assertEqual(stored["receiver_id"], previous["receiver_id"])

    async def test_repeated_import_retains_prior_pending_credential(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            path = home / "ando/connection.json"
            private_write(path, VALUE)
            pending = path.with_name("connection-pending.json")
            first = dict(VALUE, api_key="first-rotated-key")
            private_write(pending, first)
            args = SimpleNamespace(invite_stdin=False, credential_stdin=True, name=None)
            with (
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(dict(VALUE, api_key="bad-key")))),
                patch.object(Transport, "fail", True),
            ):
                with self.assertRaises(OSError):
                    await connect(args, home, {}, lambda value: self.fail("saved config"))
            backups = list(path.parent.glob("connection-recovery-*.json"))
            self.assertEqual(len(backups), 1)
            self.assertEqual(read_connection(backups[0]), first)
            self.assertEqual(backups[0].stat().st_mode & 0o777, 0o600)
            self.assertEqual(read_connection(path), VALUE)

    async def test_verified_invitation_clears_stale_pending_import(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            path = home / "ando/connection.json"
            private_write(path, VALUE)
            pending = path.with_name("connection-pending.json")
            private_write(pending, dict(VALUE, api_key="stale-key"))
            args = SimpleNamespace(invite_stdin=True, credential_stdin=False, name=None)
            with (
                patch("plugin.cli.AndoTransport", Transport),
                patch("plugin.cli.redeem_invitation", AsyncMock(return_value=VALUE)),
                patch("sys.stdin", io.StringIO("https://agents.ando.so/invite")),
                redirect_stdout(io.StringIO()),
            ):
                await connect(args, home, {}, lambda value: None)
            self.assertFalse(pending.exists())
            self.assertEqual(read_connection(path), VALUE)
