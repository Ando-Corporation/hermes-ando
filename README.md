# Ando for Hermes

The Ando plugin connects your existing Hermes agent through the same invitation
used by other agents. Copy **Invite members > Agent**, give the link to your
bot, and let it follow the invitation. No separate Ando pairing, setup download,
or OAuth approval is required for this flow.

**Public beta, listed in the Hermes community plugin catalog.** Catalog upgrades
are reviewed SHA updates. See
[the releases](https://github.com/Ando-Corporation/hermes-ando/releases) for immutable installation pins and [RELEASING.md](RELEASING.md) for remaining release gates.

## Runtime setup

Install the published, pinned Git release using Hermes's normal plugin manager
and enable `ando-platform`. Before publication, maintainers can export this
package into an isolated Hermes profile's `plugins/ando-platform` directory, then
run `hermes plugins enable ando-platform` in that profile. Dependencies are
declared in `plugin.yaml`; use Hermes's dependency installation and approval
flow. Installing alone does not grant access or verify replies.

The bot can give the existing invite URL privately to `hermes ando connect
--invite-stdin --name "Bot name"`. For an existing-agent invitation, omit the name.
If it already redeemed the invitation, pass the complete saved JSON response
privately to `hermes ando connect --credential-stdin`. Do not paste the key into
chat or put the URL/key in shell arguments. Use stdin from the bot's private
credential store. Both commands configure the same connection.

After a failure, `hermes ando connect` resumes saved credentials without consuming
another invitation. If the redemption response was lost, use the existing
reconnect invitation from **Settings > Members**. Reconnect rotates the old key
and preserves the agent identity. Stop the gateway before changing credentials.

The bot completes the invitation's introduction once. The connector does not
send another introduction. Start or restart the existing Hermes gateway, then
send a DM and follow-up while it is idle. Keep the host running; a server can
receive while your laptop is off. Stop old inbox polling routines for this agent.
If HTTPS receiving is configured, use existing Message delivery controls to
switch to WebSocket before enabling the plugin.

### Managed and Docker deployments

If the gateway runs with `GATEWAY_RELAY_URL` (for example a Nous-managed
deployment), Hermes disables all directly connected platforms, including Ando,
and only logs a warning. Set `GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS=true` in the
profile's `.env` before starting the gateway.

Run `hermes plugins install`, `hermes plugins enable` and `hermes ando ...`
through the same entrypoint and `HERMES_HOME` as the gateway; in the Docker
image that is the `/opt/hermes/bin/hermes` wrapper. The virtual environment's
`hermes` without `HERMES_HOME` reads `~/.hermes`, a different profile that does
not have this plugin. Enabling the plugin lets Hermes prepare dependencies in its
shared environment, which can re-resolve the whole environment; leave about 2 GB
free on small containers.

## Receiving and recovery

API-key receiving uses an outbound Realtime connection and Ando's direct/updates
inbox. On startup it subscribes before sweeping pending work, follows every inbox
and history page, and periodically reconciles without running the model for an
empty inbox. Realtime events are acknowledged as soon as they arrive, and a
dropped or rotated socket reconnects while a running turn finishes. If the
server explicitly reports a resume cursor older than its 24-hour replay window, invitation mode opens a fresh socket and recovers pending
work through the inbox before handling queued live events. Delivery/outbox state
is retained. Authorization errors and legacy realtime-only connections still
fail closed. Ando's existing permissions and inbox decide which DMs, mentions,
and replies need attention; the invitation's initial channels are not an
independent allowlist. No arbitrary channel joining is performed.

The local host defaults to accepting messages only from the member who connected
the agent, including existing invitation profiles created before this default.
Other workspace members and agents cannot start a Hermes turn by default.
To broaden access, explicitly set `gateway.platforms.ando.extra.allowed_users`
to a list of Ando workspace membership IDs. `ANDO_ALLOWED_USERS` overrides that
list with comma-separated membership IDs. `ANDO_ALLOW_ALL_USERS=true` or
`extra.allowed_users: ["*"]` explicitly admits all senders with Ando access.
An empty list denies everyone. Restrict channels separately with
`extra.allowed_conversations` (conversation IDs); omitted/empty means no local
channel restriction. Restart the gateway after changing these settings.
Admitted senders can run the configured Hermes tools on your host; widen access
only to members you intend to grant that capability. Ando permissions still apply.

The backend must support the optional `execution_id` field on
`acknowledge_agent_inbox_item`. The plugin refuses to generate until Ando confirms
its durable claim. A different receiver cannot acquire or acknowledge that work,
even after a new message changes its revision. Claims never time out and rerun
unknown tool effects. Keep this Hermes profile and its receiver identity when
moving hosts. Other runtimes must not poll the same work concurrently.

Ando returns one inbox item per message with `recovery.tool: "get_message"`;
the plugin answers that message in its own turn, with the thread root as context
for a reply. Older grouped items with `get_messages_by_time_range` recover their
full history window. An item with another recovery tool is left unread and
logged as a warning rather than acknowledged, so a newer plugin can answer it.

The gateway log records each Realtime connection and event, each inbox item with
its recovery tool and handling state, each turn started and reply sent, and a
warning whenever `allowed_users` or `allowed_conversations` skips a message.
DEBUG logging adds frame acknowledgements and empty sweeps.

Message IDs deduplicate live/recovery overlap. Replies are frozen in SQLite
before sending and retried with identical idempotency keys and content. A turn
that stops before its reply is prepared (a crash, a restart or a failed
generation) is never regenerated, because its tools may already have run.
Instead the agent replies once that it stopped and asks the member to send the
message again; receiving continues. To retry the turn itself, stop the gateway,
review previous tool effects and run `hermes ando recover EVENT_ID
--previous-effects-reviewed` in the original profile before restarting. A lost
profile requires restoring its protected backup; do not invent a new receiver
identity to bypass an unfinished claim. Network delivery is at least once; this
is not a promise of exactly-once execution of arbitrary model tools.

Ando gets the final reply only: the plugin registers Hermes's `low` display
tier, which turns off tool progress, interim commentary and the "⏳ Working"
heartbeat. Ando's 👀 on the message shows the turn started. To keep a heartbeat
for long turns, set `display.platforms.ando.long_running_notifications: true`
in the profile's config.yaml; it is posted once and then edited in place, and,
like any status Hermes marks as interim, it is never stored or resent as the
reply. Keep `tool_progress` off: Hermes sends tool progress unmarked, so this
plugin would treat it as the reply. Hermes's gateway notices (no home channel,
credits, failed subagents) stay in the gateway log rather than Ando: the plugin
defaults `notice_delivery` to `private`, and Ando has no member-only notice.

Credentials live in the profile's `ando/connection.json`, written atomically with
mode 600. SQLite and recovery metadata are also outside the plugin's upgrade
directory. Treat the whole profile as private and retain it across upgrades.
Never copy a live profile to two running hosts. `hermes ando status` prints only
identity and configuration; `hermes ando doctor` checks dependencies and identity.

`hermes ando test` checks what would stop this profile receiving: the plugin and
its dependencies, `GATEWAY_RELAY_URL` without
`GATEWAY_RELAY_ALLOW_DIRECT_PLATFORMS=true`, the credential, the inbox (and any
items this plugin cannot answer) and, with the gateway stopped, that Realtime
subscribes. `hermes ando test --wait 120` then waits for a message you send the
agent in Ando and reports when it arrives. The test claims and acknowledges
nothing, so the gateway still answers that message when it starts. It exits
non-zero when a check fails. `hermes ando disconnect` disables
the configuration; restart the gateway to apply, retaining identity and state.

This release is sequential. The receiver does not install all
Ando tools in the model. Hermes keeps its own model/tool configuration; the
adapter enforces Ando restrictions it can implement and stops when host-tool
preferences cannot be enforced. Streaming edits, proactive sends,
and automatic OS service installation are outside this release.

## Files

Files go both ways in the conversation being answered.

Files Hermes sends (`MEDIA:` paths, images in the reply, `send_document`,
`send_video`, `send_voice`, `send_image`) become Ando attachments on their own
reply after the text. A batch of images is one message (up to 10 per message).
Remote images are downloaded on the host first, through Hermes's SSRF-checked
downloader. Ando accepts PNG, JPEG, GIF, WEBP, PDF, MP4, MOV, WebM, MP3, WAV,
Ogg Vorbis, ZIP, DOCX, XLSX, PPTX, plain text, Markdown, CSV and JSON up to
20 MB each. The type is decided from the file's bytes, not its extension.
Anything else, including Opus `.ogg` and `.m4a` voice notes, is not uploaded,
and Hermes posts its usual "Couldn't deliver" notice instead. An agent can
request at most 20 uploads in 10 minutes.

Attachments are part of the durable reply. Which local files a reply carries
is recorded before upload, and uploads use idempotency keys derived from that
reply. After a crash the gateway finishes the same reply without uploading
finished bytes again or posting twice. A file that changed or disappeared
before the restart becomes the "Couldn't deliver" text.

Files a member attaches are downloaded when the message arrives, because their
download links last about a minute. They are cached in the profile's Hermes
cache, so images reach Hermes vision and other files appear as paths the model
can open. At most 10 files of up to 20 MB each are read per message; anything
not read is named in the message text. Turning off `upload_attachment` or
`get_file` in **Settings > Members** stops files in that direction only; the
turn still runs. Upload and download links get no Ando credential, are never
followed through redirects, and are never logged.

## Compatibility and tests

Validated against the actual Hermes source used in the release evidence. The
current candidate test targets are Hermes main
`e6cf2a9b1e8f98d40408b09a9a55993c8459b4b4` and releases v0.22.2 and v0.21.7,
Python 3.14, MCP 2.0.0, httpx2 2.12.0, and websockets 15.0.1. plugin.yaml
declares bounded ranges rather than pins, so a Hermes that moved to a newer
httpx2 can still install it. Hermes before v0.21.6 has no display tiers: the
plugin still loads there but cannot keep tool progress out of Ando, and logs a
warning to update Hermes.
This is source compatibility evidence, not a claim that all Hermes versions or
operating systems are supported.
The current private-file lock implementation targets macOS/Linux.

```sh
python -m unittest discover -s packages/hermes-ando/tests -v
HERMES_SOURCE=/path/to/hermes python -m unittest discover -s packages/hermes-ando/tests -v
```

In the exported public repository, use `-s tests` instead. Provider tests must
run without skips for a release. They use real Hermes lifecycle and MCP code
with controlled model/HTTP responses; a live workspace round trip remains a
separate release gate.

## Existing OAuth preview installations

Keep the existing identity and profile. The previous OAuth setup helper remains
available for recovery; see [LEGACY-OAUTH.md](LEGACY-OAUTH.md). New users follow
the invitation instructions above.

### Interrupted credential replacement

A reconnect import keeps the verified connection until the replacement passes
identity checks. The replacement is stored privately in
`$HERMES_HOME/ando/connection-pending.json`; plain `hermes ando connect` retries it.
Importing another replacement preserves the earlier pending response in a private
`connection-recovery-<uuid>.json` file in the same directory. To retry that response,
feed that file to `hermes ando connect --credential-stdin`. A successful connection
clears the pending file. Recovery copies remain private for operator review.
