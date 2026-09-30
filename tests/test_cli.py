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
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.cli import connect
from plugin.connection import read_connection
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
