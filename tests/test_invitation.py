import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from plugin.connection import (
    invitation_url,
    private_write,
    read_connection,
    redeem_invitation,
)
from plugin.protocol import SetupRequired

MEMBER = "0acaf9b1-57b3-4948-9adf-cb64b6483482"
VALUE = {
    "contract_version": 1,
    "api_key": "test-private-key",
    "workspace_id": "workspace",
    "agent_membership_id": MEMBER,
    "connected_by_membership_id": "human",
    "mcp_url": "https://mcp.ando.so/mcp",
}
URL = "https://agents.ando.so/invite/abcdefghjk"


class Client:
    def __init__(self, existing=False):
        self.posts = []
        self.existing = existing

    async def get(self, url):
        text = (
            f'Connect to Ando as "Ada" (agent {MEMBER}); keep this identity.'
            if self.existing
            else "Join Ando as a new agent"
        )
        return SimpleNamespace(status_code=200, text=text)

    async def post(self, url, json):
        self.posts.append((url, json))
        return SimpleNamespace(status_code=200, json=lambda: copy.deepcopy(VALUE))


class InvitationTests(unittest.IsolatedAsyncioTestCase):
    async def test_redemption_is_private_and_retry_does_not_consume_again(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connection.json"
            client = Client()
            first = await redeem_invitation(URL, "Ada", path, client)
            again = await redeem_invitation(URL, "Ada", path, client)
            self.assertEqual(first, again)
            self.assertEqual(len(client.posts), 1)
            self.assertEqual(client.posts[0][1]["harness"], "hermes")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(read_connection(path)["api_key"], VALUE["api_key"])

    async def test_existing_identity_omits_name_and_preserves_receiver_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connection.json"
            private_write(path, {**VALUE, "receiver_id": "owner"})
            client = Client(existing=True)
            value = await redeem_invitation(URL, "different-name", path, client)
            self.assertNotIn("name", client.posts[0][1])
            self.assertEqual(value["receiver_id"], "owner")

    async def test_occupied_profile_refuses_new_invitation_before_redemption(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connection.json"
            private_write(path, VALUE)
            client = Client()
            with self.assertRaises(SetupRequired):
                await redeem_invitation(URL, "Ada", path, client)
            self.assertEqual(client.posts, [])

    def test_rejects_untrusted_invitation_destinations_and_secret_file_permissions(
        self,
    ):
        for url in [
            URL.replace("https", "http"),
            URL.replace("agents.ando.so", "evil.invalid"),
            URL + "?redirect=x",
            URL + "#x",
        ]:
            with self.assertRaises(SetupRequired):
                invitation_url(url)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connection.json"
            private_write(path, VALUE)
            path.chmod(0o644)
            with self.assertRaises(SetupRequired):
                read_connection(path)
