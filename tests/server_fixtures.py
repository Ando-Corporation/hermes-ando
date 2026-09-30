"""The paired-agent OAuth directory shape, not the legacy human directory.

Owner: packages/shared/src/capabilities/conversation-read.ts
       ConversationDirectoryItem
Envelope: apps/express/mcp/tools/list-conversations.ts
          hasConnectedAgentAccess branch (paired agent OAuth).

The legacy list has items[*].id; the connected-agent directory intentionally
has conversation_id instead. Keep setup and runtime on the same actual contract.
"""


def conversation_directory(conversation_id, *, next_cursor=None):
    return {
        "items": [
            {
                "conversation_id": conversation_id,
                "kind": "direct_message",
                "visibility": "private",
                "name": "Installer DM",
                "description": None,
                "joined": True,
                "can_read": True,
                "can_post": True,
                "archived": False,
                "active": None,
                "activity_window_start_at": None,
                "activity_window_end_at": None,
                "activity_definition_version": None,
                "last_activity_at": None,
                "is_bridge": False,
            }
        ],
        "page": {"has_more": next_cursor is not None, "next_cursor": next_cursor},
        "coverage": {
            "scope": "directory",
            "omitted_count": 0,
            "content_truncated": False,
        },
    }
