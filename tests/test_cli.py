"""Installed CLI preserves credentials and unrelated Hermes configuration."""

import asyncio
import copy
from contextlib import asynccontextmanager, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.cli import connect, execute, receiving_test
from plugin.connection import read_connection, private_write
from plugin.protocol import SetupRequired, lock_acquired
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
            self.assertEqual(config["gateway"]["platforms"]["ando"]["extra"]["allowed_users"],
                             [VALUE["connected_by_membership_id"]])
            self.assertNotIn(VALUE["api_key"], output.getvalue())

    async def test_failed_reconnect_preserves_working_credential(self):
        import json

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            path = home / "ando/connection.json"
            previous = dict(VALUE, receiver_id="12345678-1234-4234-8234-123456789012")
            private_write(path, previous)
            replacement = dict(VALUE, api_key="replacement-key")
            config = {
                "gateway": {
                    "platforms": {
                        "ando": {
                            "enabled": True,
                            "extra": {
                                "workspace_id": VALUE["workspace_id"],
                                "membership_id": VALUE["agent_membership_id"],
                                "allowed_users": [],
                                "allowed_conversations": ["conversation-1"],
                            },
                        }
                    }
                }
            }
            saved = []
            args = SimpleNamespace(invite_stdin=False, credential_stdin=True, name=None)
            with (
                patch("plugin.cli.preflight", lambda value: None),
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(replacement))),
                patch.object(Transport, "fail", True),
            ):
                with self.assertRaises(OSError):
                    await connect(args, home, config, lambda value: self.fail("saved config"))
            self.assertEqual(read_connection(path), previous)
            pending = path.with_name("connection-pending.json")
            self.assertEqual(read_connection(pending)["api_key"], replacement["api_key"])
            self.assertEqual(pending.stat().st_mode & 0o777, 0o600)
            args.credential_stdin = False
            with (
                patch("plugin.cli.preflight", lambda value: None),
                patch("plugin.cli.AndoTransport", Transport),
                patch("sys.stdin", io.StringIO(json.dumps(replacement))),
                redirect_stdout(io.StringIO()),
            ):
                await connect(args, home, config, lambda value: saved.append(copy.deepcopy(value)))
            stored = read_connection(path)
            self.assertFalse(pending.exists())
            self.assertEqual(stored["api_key"], replacement["api_key"])
            self.assertEqual(stored["receiver_id"], previous["receiver_id"])
            self.assertEqual(len(saved), 1)
            extra = saved[0]["gateway"]["platforms"]["ando"]["extra"]
            self.assertEqual(extra["allowed_users"], [])
            self.assertEqual(extra["allowed_conversations"], ["conversation-1"])

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


class Socket:
    def __init__(self, frames):
        self.frames = [json.dumps(frame) for frame in frames]
        self.sent = []

    async def recv(self):
        if not self.frames:
            await asyncio.sleep(3600)
        return self.frames.pop(0)

    async def send(self, value):
        self.sent.append(value)


class TestTransport(Transport):
    inbox = []
    later_inbox = []
    receiving_method = None

    async def call(self, name, args):
        if name == "get_agent_inbox":
            if args["scope"] != "direct":
                return {"items": [], "page": {"has_more": False}}
            if "cursor" in args:
                return {"items": self.later_inbox, "page": {"has_more": False}}
            more = bool(self.later_inbox)
            page = {"has_more": more, "next_cursor": "page-2" if more else None}
            return {"items": self.inbox, "page": page}
        result = await super().call(name, args)
        if name == "get_workspace_info" and self.receiving_method:
            result["delivery"] = {"receiving": {"configured_method": self.receiving_method}}
        return result

    async def ticket(self, cursor):
        assert cursor is None
        return {"url": "wss://realtime.ando.so/socket"}


def message_event(author):
    return {
        "type": "event",
        "envelope_id": "envelope",
        "payload": {
            "id": "event",
            "type": "message.created",
            "workspace_id": VALUE["workspace_id"],
            "data": {
                "object": {
                    "id": "message",
                    "conversation_id": "conversation",
                    "authorWorkspaceMembershipId": author,
                }
            },
        },
    }


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE for installed CLI dependency checks",
)
class ReceivingTestCommand(unittest.IsolatedAsyncioTestCase):
    CONFIG = {
        "gateway": {
            "platforms": {
                "ando": {"enabled": True, "extra": {"credential_mode": "invitation"}}
            }
        }
    }

    def setUp(self):
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        TestTransport.inbox = []
        TestTransport.later_inbox = []
        TestTransport.receiving_method = None

    async def run_test(self, socket, *, wait=0, lock=(True, None), env=None):
        @asynccontextmanager
        async def connect(*args, **kwargs):
            yield socket

        output = io.StringIO()
        exit_code = 0
        with (
            patch("plugin.cli.AndoTransport", TestTransport),
            patch("plugin.cli.preflight", lambda config: None),
            patch("gateway.status.acquire_scoped_lock", return_value=lock),
            patch("gateway.status.release_scoped_lock") as release,
            patch.dict(os.environ, env or {}, clear=False),
            redirect_stdout(output),
        ):
            try:
                await receiving_test(
                    SimpleNamespace(wait=wait),
                    self.CONFIG,
                    VALUE,
                    socket_connect=connect,
                )
            except SystemExit as exc:
                exit_code = exc.code
        return output.getvalue(), exit_code, release

    async def test_waits_for_a_real_message_without_acknowledging_it(self):
        socket = Socket(
            [{"type": "hello"}, message_event(VALUE["agent_membership_id"]),
             message_event("human")]
        )
        output, code, release = await self.run_test(socket, wait=5)
        self.assertEqual(code, 0)
        self.assertIn("✓ Message message arrived over Realtime", output)
        self.assertEqual(socket.sent, [])
        release.assert_called_once()

    async def test_reports_relay_and_unanswerable_inbox_items(self):
        TestTransport.inbox = [
            {"type": "direct_message", "recovery": {"tool": "get_message"}},
            {"type": "reply", "recovery": {"tool": "future_tool"}},
        ]
        output, code, _ = await self.run_test(
            Socket([{"type": "hello"}]),
            env={"GATEWAY_RELAY_URL": "https://relay.invalid"},
        )
        self.assertEqual(code, 1)
        self.assertIn("GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS=true", output)
        self.assertIn("2 message item(s) waiting, 1 this plugin can't answer", output)

    async def test_counts_waiting_items_on_later_inbox_pages(self):
        TestTransport.inbox = [{"type": "direct_message"}]
        TestTransport.later_inbox = [{"type": "mention", "recovery": {"tool": "future_tool"}}]
        output, code, _ = await self.run_test(Socket([{"type": "hello"}]))
        self.assertEqual(code, 1)
        self.assertIn("2 message item(s) waiting, 1 this plugin can't answer", output)

    async def test_reports_an_https_receiver_the_gateway_would_refuse(self):
        TestTransport.receiving_method = "https"
        output, code, _ = await self.run_test(Socket([{"type": "hello"}]))
        self.assertEqual(code, 1)
        self.assertIn("✗ Ando sends this agent's messages over WebSocket", output)

    async def test_leaves_realtime_to_a_running_gateway(self):
        socket = Socket([{"type": "hello"}])
        output, code, release = await self.run_test(socket, lock=(False, {"pid": 1}))
        self.assertEqual(code, 0)
        self.assertIn("The gateway is running this receiver", output)
        self.assertEqual(len(socket.frames), 1)
        release.assert_not_called()

    async def test_missing_subscription_confirmation_fails(self):
        with patch("plugin.cli.HELLO_TIMEOUT_SECONDS", 0.01):
            output, code, _ = await self.run_test(Socket([]))
        self.assertEqual(code, 1)
        self.assertIn("never confirmed the subscription", output)


    async def test_refuses_a_config_naming_another_agent(self):
        config = copy.deepcopy(self.CONFIG)
        config["gateway"]["platforms"]["ando"]["extra"].update(
            workspace_id=VALUE["workspace_id"], membership_id="another-agent"
        )
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            private_write(home / "ando" / "connection.json", VALUE)
            with (
                patch("plugin.cli.profile_home", return_value=home),
                patch("hermes_cli.config.load_config", return_value=config),
                patch("plugin.cli.receiving_test") as test,
            ):
                with self.assertRaisesRegex(SetupRequired, "different Ando agents"):
                    await execute(SimpleNamespace(operation="test", wait=0))
        test.assert_not_called()


class LockResult(unittest.TestCase):
    def test_reads_tuple_and_bool_results(self):
        self.assertFalse(lock_acquired((False, {"pid": 1})))
        self.assertTrue(lock_acquired((True, None)))
        self.assertTrue(lock_acquired(True))


@unittest.skipUnless(
    os.environ.get("HERMES_SOURCE"),
    "Set HERMES_SOURCE for installed CLI dependency checks",
)
class RecoverScript(unittest.TestCase):
    def test_refuses_while_the_gateway_holds_the_receiver(self):
        sys.path.insert(0, os.environ["HERMES_SOURCE"])
        import recover

        argv = [
            "recover.py", "event-1",
            "--workspace-id", "00000000-0000-4000-8000-000000000001",
            "--membership-id", "00000000-0000-4000-8000-000000000002",
            "--previous-effects-reviewed",
        ]
        with (
            patch.object(sys, "argv", argv),
            patch("gateway.status.acquire_scoped_lock", return_value=(False, {"pid": 1})),
            patch("gateway.status.release_scoped_lock") as release,
            patch("recover.DeliveryState") as state,
            redirect_stdout(io.StringIO()),
            patch("sys.stderr", io.StringIO()) as stderr,
        ):
            with self.assertRaises(SystemExit):
                recover.main()
        self.assertIn("Stop the Ando gateway", stderr.getvalue())
        state.assert_not_called()
        release.assert_not_called()
