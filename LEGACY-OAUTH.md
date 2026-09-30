# Existing OAuth preview installations

Compatibility instructions for installations made before the invitation plugin.
New installations follow [README.md](README.md). Historical Studio references
below describe the original preview, not the current invitation user interface.

These remain supported as a compatibility path. New users use the invitation
above. Do not replace a working identity to upgrade. The old source helper is
retained for recovery of those installations only.

This native Hermes platform plugin receives messages in the installer's Ando DM,
starts a Hermes turn while the gateway is idle, and posts the final text as the
paired Ando agent. The browser can close after OAuth. The gateway's host must stay
running; a hosted Hermes gateway can run independently of a user's laptop.

**Status: source preview, not published or live-provider verified.** Do not describe
OAuth success, plugin installation, or an open socket as a verified reply path.
The shared Ando onboarding/Studio flow owns that distinction.

## Install from this checkout

Use an installed Hermes environment with MCP support. This preview was checked
against official Hermes source
[`b1f003e18633298d549668b8e186af84cca45b76`](https://github.com/NousResearch/hermes-agent/tree/b1f003e18633298d549668b8e186af84cca45b76),
Python 3.12, MCP Python SDK 2.0.0 and websockets 15.0.1. It depends on Hermes's
platform lifecycle and OAuth manager APIs; compatibility with older Hermes
releases is not established. No hypothetical PyPI/plugin-index package is used.

1. In Ando, select Hermes and download the setup file for this agent. New first-agent
   onboarding uses `mode: create`, an attempt-scoped MCP URL and the requested name.
   Studio has already created the profile, so it uses `mode: connect` with that
   exact membership ID. Connecting an existing agent also uses `mode: connect`.
2. Use the Python interpreter belonging to the intended Hermes environment/profile:

   ```sh
   python /path/to/ando/packages/hermes-ando/setup.py ~/Downloads/ando-hermes-setup.json
   ```

3. Approve OAuth in the browser. For `create`, choose **Create new agent** and the
   requested name; for `connect`, choose the exact existing agent named by Ando.
   Return to the terminal even if the callback page is blank. The helper checks
   actual identity/workspace responses before enabling the runtime.
4. Start the receiver in that same Hermes profile:

   ```sh
   hermes gateway run
   ```

5. Send a **new** message in the existing Ando DM once setup is idle. Send a
   follow-up and confirm the context is retained. Only this proves the conversation
   works. A setup-generated introduction does not prove incoming-message delivery.

**First-start limitation:** the initial realtime connection listens from now. It
does not drain earlier inbox activity. Resend any request sent before the first
gateway startup. Subsequent starts resume the persisted cursor; an expired or
rejected cursor requires explicit recovery, never an automatic reset.

The helper copies `plugin/` into the selected Hermes home's
`plugins/ando-platform`, enables it, and configures `gateway.platforms.ando`.
It preserves unrelated configuration and refuses to assign a different Ando agent
to an occupied Hermes profile. Use another Hermes profile for another agent.
Ordinary retries reuse the exact resource and cached grant. `--skip-login` is
available when OAuth was already completed. If reauthorization is necessary, stop
the receiver and explicitly run Hermes's own MCP login for the configured server;
the adapter never starts a fresh browser flow in the background.

Setup is resumable: it discovers the installer and existing exact two-member DM
with read-only tools, and stops if either is missing. It does not create a second
DM, send an introduction, join channels, set an avatar or share resources. The
initial allowlist is that installer and DM. Additional conversations/senders
require an explicit operator configuration change.

## Creation parity contract

All three entry points use the same helper and runtime. The setup download is:

```json
{
  "schemaVersion": 1,
  "harness": "hermes",
  "mode": "connect",
  "mcpUrl": "<the exact URL supplied by Ando>",
  "workspaceId": "<workspace UUID>",
  "displayName": "<chosen agent name>",
  "workspaceMembershipId": "<exact agent membership UUID>"
}
```

| Entry point            | Identity check                                                 | Human recovery                                                                  |
| ---------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| First-agent onboarding | Agent type, workspace, requested name; pin returned membership | Resume the same issuance; generate a new setup only if Ando expires/restarts it |
| Studio new agent       | Agent type, workspace, exact newly-created membership          | Reopen runtime setup for that profile; do not create another agent in OAuth     |
| Connect existing       | Agent type, workspace, exact existing membership               | Reauthorize the same profile; no implicit agent replacement                     |

On each path, show the source-preview prerequisite, local setup command and
running-host requirement before commitment. Keep optional resource sharing after
the DM round trip. UI changes live in the common Ando setup owner, not this plugin.

## Runtime and delivery guarantees

- OAuth discovery, PKCE, token storage and refresh belong to Hermes. The exact MCP
  resource URL is retained, including pairing scope. The realtime ticket endpoint
  is `/realtime/connections` on that same origin; ticket URLs are never logged.
- A dedicated `ando-gateway-<resource hash>` MCP entry has `enabled: false` for
  Hermes's model-tool discovery. The native adapter still uses it. This avoids
  cross-event-loop OAuth lock sharing and duplicate model-issued sends. It does
  **not** automatically attach the full Ando toolset to the model. Initial context
  consists of each fetched message and Hermes's conversation history.
- The receiver verifies agent/workspace/membership before each connection. It
  also requires the same resolved installer. It rechecks conversation access and
  Studio tool preferences before each new event. Adapter read/reply restrictions
  are enforced locally. If Studio disables a Hermes host tool this adapter cannot
  filter per turn, it stops **before generation** and requires configuration
  recovery; it never substitutes prompt guidance for enforcement or changes the
  global Hermes tool configuration.
- Incoming text cannot execute Hermes gateway control commands (`/restart`,
  `/approve`, etc.). Only allowlisted senders/conversations can start work.
- `handle_message` only starts background work in Hermes. The adapter waits for
  `on_processing_complete` **and a confirmed Ando reply** before acknowledging.
  Empty, failed or cancelled turns do not acknowledge success.
- Sessions are keyed by workspace, agent, conversation and thread. One receiver
  serves one agent; a scoped process lock prevents concurrent profiles serving it.
- SQLite freezes reply bytes and an event-specific idempotency key before sending.
  Replayed completed events do not run again. Uncertain sends retry identical
  content instead of regenerating. Network delivery is at least once; this does
  not promise exactly-once model/tool execution. Generation-start is persisted
  before dispatch. A crash/failure before a reply was prepared stops replay and
  requires operator review, so prior tools are not automatically run again.
- Only server-confirmed `acknowledged` or planned-disconnect cursors advance
  replay state, plus the authoritative initial floor from the ticket. An event's
  cursor is never treated as success. Invalid/stale replay or revoked access stops
  with recovery required; it never silently starts fresh and loses messages.

The first implementation is text-focused, sequential and limited to the installer
DM. File upload, streaming message edits, proactive sends and automatic operating
system service installation are outside this preview. The state database under
the Hermes home's `ando/` directory contains private message text and should be
retained across restarts; deleting it discards deduplication/reply recovery.

For an interrupted generation, stop the gateway and inspect that Hermes turn's
tool effects. Only after deciding it is safe to retry, run:

```sh
python /path/to/ando/packages/hermes-ando/recover.py EVENT_ID \
  --workspace-id WORKSPACE_UUID --membership-id AGENT_UUID \
  --previous-effects-reviewed
```

Then restart the gateway. The error identifies the event ID. This explicit local
recovery cannot clear a frozen or completed reply and refuses to run while the
same agent receiver is active. It authorizes rerunning the original generation;
the operator must account for effects that already occurred.

## Validation

Provider-independent delivery/setup tests use Python's standard library:

```sh
python -m unittest discover -s packages/hermes-ando/tests -v
```

For real Hermes lifecycle and OAuth compatibility tests, use an isolated environment
with the dependency versions above, and set `HERMES_SOURCE` to the verified source:

```sh
HERMES_SOURCE=/path/to/verified/hermes python -m unittest discover -s packages/hermes-ando/tests -v
```

Those tests run the actual Hermes base adapter's background lifecycle and its
OAuth manager/MCP SDK with a mock HTTP transport, including expired-token refresh,
exact scoped resource requests and authenticated realtime ticket creation. They
do not invoke an LLM, pair a live agent or prove production event delivery.

Before release, collect an idle DM round trip and follow-up from each creation
entry point; close the browser; restart the gateway around an uncertain send;
verify revocation and reconnect behavior. These need fresh browser consent and a
running Hermes/model environment. No deployment, publication or live write is
performed by this package's test suite.

Roll out as an explicitly labeled source preview first. Roll back by stopping the
gateway and disabling `ando-platform` in Hermes; keep delivery state for recovery.
Revoke the Ando connection only when the operator intends to revoke access.

## Sources

- [Hermes platform adapter guide](https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters)
- [Hermes MCP and OAuth guide](https://hermes-agent.nousresearch.com/docs/user-guide/features/mcp)
- [Hermes plugin lifecycle](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins)
- [Ando external-agent guide](https://docs.ando.so/docs/external-agents)
