import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.protocol import SetupRequired
from server_fixtures import conversation_directory

spec = importlib.util.spec_from_file_location(
    "ando_setup", Path(__file__).resolve().parents[1] / "setup.py"
)
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)

WORKSPACE = "6417f06f-717f-4ec0-ae54-fd4044520444"
MEMBER = "0acaf9b1-57b3-4948-9adf-cb64b6483482"
REQUEST = {
    "schemaVersion": 1,
    "harness": "hermes",
    "mode": "create",
    "workspaceId": WORKSPACE,
    "displayName": "Ada",
    "mcpUrl": "https://mcp.ando.so/mcp/pair/test-issuance",
}


class SetupTests(unittest.IsolatedAsyncioTestCase):
    def test_connect_requires_exact_existing_member_and_create_preserves_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "setup.json"
            path.write_text(json.dumps(REQUEST))
            self.assertEqual(setup.read_setup(path), REQUEST)
            path.write_text(json.dumps({**REQUEST, "mode": "connect"}))
            with self.assertRaises(KeyError):
                setup.read_setup(path)
            path.write_text(
                json.dumps(
                    {**REQUEST, "mode": "connect", "workspaceMembershipId": MEMBER}
                )
            )
            self.assertEqual(setup.read_setup(path)["workspaceMembershipId"], MEMBER)

    def test_setup_reuses_only_its_own_exact_resource_and_preserves_other_connections(
        self,
    ):
        existing = {
            "url": "https://other.invalid/mcp",
            "auth": "oauth",
            "enabled": True,
        }
        config = {"mcp_servers": {"work-tools": copy.deepcopy(existing)}}
        name = setup.prepare_server(config, REQUEST)
        self.assertEqual(config["mcp_servers"]["work-tools"], existing)
        self.assertEqual(config["mcp_servers"][name]["url"], REQUEST["mcpUrl"])
        self.assertFalse(config["mcp_servers"][name]["enabled"])
        self.assertEqual(setup.prepare_server(config, REQUEST), name)
        next_name = setup.prepare_server(
            config, {**REQUEST, "mcpUrl": "https://mcp.ando.so/mcp/pair/next-attempt"}
        )
        self.assertNotEqual(name, next_name)

    def test_new_and_existing_agent_runtime_pin_uses_verified_member_and_only_installer_dm(
        self,
    ):
        for request in [
            REQUEST,
            {**REQUEST, "mode": "connect", "workspaceMembershipId": MEMBER},
        ]:
            config = {
                "gateway": {"platforms": {"existing-platform": {"enabled": True}}}
            }
            setup.runtime_config(
                config,
                request,
                "server",
                {"workspace_membership_id": MEMBER},
                "installer",
                "dm",
            )
            self.assertTrue(
                config["gateway"]["platforms"]["existing-platform"]["enabled"]
            )
            ando = config["gateway"]["platforms"]["ando"]
            self.assertEqual(ando["extra"]["membership_id"], MEMBER)
            self.assertEqual(ando["extra"]["allowed_conversations"], ["dm"])
            self.assertFalse(ando["gateway_restart_notification"])

    def test_runtime_reconfiguration_replaces_stale_trigger_allowlists(self):
        config = {
            "gateway": {
                "platforms": {
                    "ando": {
                        "extra": {
                            "membership_id": MEMBER,
                            "allowed_users": ["former-installer"],
                            "allowed_conversations": ["former-dm"],
                        }
                    }
                }
            }
        }

        setup.runtime_config(
            config,
            REQUEST,
            "server",
            {"workspace_membership_id": MEMBER},
            "current-installer",
            "current-dm",
        )

        extra = config["gateway"]["platforms"]["ando"]["extra"]
        self.assertEqual(extra["installer_id"], "current-installer")
        self.assertEqual(extra["allowed_users"], ["current-installer"])
        self.assertEqual(extra["allowed_conversations"], ["current-dm"])

    async def test_installer_dm_lookup_ignores_group_dm_and_pages_without_writing(self):
        calls = []

        class Transport:
            async def call(self, name, args):
                calls.append((name, args))
                if name == "list_conversations":
                    if "cursor" not in args:
                        return conversation_directory("group", next_cursor="next")
                    return conversation_directory("dm")
                ids = ["installer", MEMBER] + (
                    ["stranger"] if args["conversation_id"] == "group" else []
                )
                return {
                    # ListConversationMembersCapabilityOutput uses this camelCase ID.
                    "items": [{"workspaceMembershipId": member} for member in ids]
                }

        self.assertEqual(
            await setup.find_installer_dm(Transport(), "installer", MEMBER), "dm"
        )
        self.assertTrue(
            all(
                name in {"list_conversations", "list_conversation_members"}
                for name, _ in calls
            )
        )

    async def test_missing_installer_dm_does_not_create_one(self):
        class Transport:
            async def call(self, name, args):
                if name != "list_conversations":
                    raise AssertionError("No writes expected")
                return {"items": [], "page": {"has_more": False}}

        with self.assertRaisesRegex(SetupRequired, "not created"):
            await setup.find_installer_dm(Transport(), "installer", MEMBER)


if __name__ == "__main__":
    unittest.main()
