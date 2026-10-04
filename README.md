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

## Receiving and recovery

API-key receiving uses an outbound Realtime connection and Ando's direct/updates
inbox. On startup it subscribes before sweeping pending work, follows every inbox
and history page, and periodically reconciles without running the model for an
empty inbox. If the server explicitly reports a resume cursor older than its
24-hour replay window, invitation mode opens a fresh socket and recovers pending
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

Message IDs deduplicate live/recovery overlap. Replies are frozen in SQLite
before sending and retried with identical idempotency keys and content. A crash
before a reply is prepared requires reviewing previous tool effects, stopping
the gateway, and running `hermes ando recover EVENT_ID
--previous-effects-reviewed` in the original profile. Restart afterward. A lost
profile requires restoring its protected backup; do not invent a new receiver
identity to bypass an unfinished claim. Network delivery is at least once; this
is not a promise of exactly-once execution of arbitrary model tools.

Credentials live in the profile's `ando/connection.json`, written atomically with
mode 600. SQLite and recovery metadata are also outside the plugin's upgrade
directory. Treat the whole profile as private and retain it across upgrades.
Never copy a live profile to two running hosts. `hermes ando status` prints only
identity and configuration; `hermes ando doctor` checks dependencies and identity.
Neither command claims receiving was verified. `hermes ando disconnect` disables
the configuration; restart the gateway to apply, retaining identity and state.

This release is text-focused and sequential. The receiver does not install all
Ando tools in the model. Hermes keeps its own model/tool configuration; the
adapter enforces Ando restrictions it can implement and stops when host-tool
preferences cannot be enforced. Attachments, streaming edits, proactive sends,
and automatic OS service installation are outside this release.

## Compatibility and tests

Validated against the actual Hermes source used in the release evidence. The
current candidate test target is commit
`38d3dbce4adbb9074a34214eb330237a129e5904`, Python 3.14, MCP 2.0.0,
httpx2 2.7.0, and websockets 15.0.1. This is source compatibility evidence,
not a claim that all Hermes versions or operating systems are supported.
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
