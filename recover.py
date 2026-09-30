"""Explicit operator recovery for a turn interrupted before a reply was prepared.

Stop the gateway and inspect prior Hermes/tool effects first. This never retries
automatically and cannot reset a frozen or completed reply. It changes only local
delivery state; the receiver retries the original event on its next resume.
"""

import argparse
from pathlib import Path
from uuid import UUID

from plugin.state import DeliveryState


def main():
    from hermes_constants import get_hermes_home

    parser = argparse.ArgumentParser(
        description="Allow retry only after reviewing an interrupted Hermes turn"
    )
    parser.add_argument("event_id")
    parser.add_argument("--workspace-id", required=True, type=UUID)
    parser.add_argument("--membership-id", required=True, type=UUID)
    parser.add_argument(
        "--previous-effects-reviewed",
        action="store_true",
        required=True,
        help="Confirm prior tool effects were inspected; retry may repeat them",
    )
    args = parser.parse_args()
    from gateway.status import acquire_scoped_lock, release_scoped_lock

    key = f"{args.workspace_id}:{args.membership_id}"
    if not acquire_scoped_lock("ando", key):
        parser.error("Stop the Ando gateway before recovering a turn")
    try:
        path = Path(get_hermes_home()) / "ando" / f"{args.membership_id}.sqlite3"
        if not path.exists():
            parser.error("No delivery state exists for this agent")
        state = DeliveryState(path, str(args.workspace_id), str(args.membership_id))
        try:
            state.retry_after_review(args.event_id)
        finally:
            state.close()
    finally:
        release_scoped_lock("ando", key)
    print("Explicit retry permitted. Restart the gateway to resume the original event.")


if __name__ == "__main__":
    main()
