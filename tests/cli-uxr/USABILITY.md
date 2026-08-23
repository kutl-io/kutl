# Agent-as-User Usability Pass

**Purpose:** the *discovery* half of this crate's outside-in loop — hand an
LLM agent the built
binaries and a naive-user goal, let it drive the real `kutl` CLI + `kutl mcp
serve` surface, and have it **judge** clarity, discoverability, and error
quality to surface externally-visible confusion the deterministic journey
suite can't assert. It produces a **findings report, not a pass/fail
gate**. This pass is **operator-triggered and billed — it is NOT wired into
CI.** Only the deterministic journey suite (`tests/journey_*.rs`) gates CI.

## How it fits the outside-in loop

```
discover (agent-as-user, this pass)  →  fix  →  pin (deterministic journey + unit test)
```

The agent-as-user pass is the formalization of the by-hand outside-in
bug-finding that produced this suite in the first place. It finds an
unknown-unknown; we fix it; we lock it down with a deterministic journey in
this crate (`tests/journey_*.rs`) and a fine-grained unit test so it can never
regress.

Concrete finds this loop has already produced (cite these to the agent as
examples of the *kind* of thing worth reporting):

- **1970 timestamps** — client/agent-authored signals rendered at epoch 0
  because authoring hardcoded `timestamp: 0`. Pinned by `journey_signals.rs`.
- **Opaque "not authorized"** — an agent's DID missing from `authorized_keys`
  produced a bare rejection; now the message names `authorized_keys` and echoes
  the offending DID. Pinned by `journey_agent.rs`.
- **`reopen_flag` not discoverable** — dispatched on the OSS MCP surface but not
  advertised in `tools/list`; now advertised. Pinned by `journey_mcp.rs`.
- **CLI signal triage couldn't auth to a self-hosted relay** — `signal
  close/reopen/resolve` had no did:key challenge; now it authenticates via
  challenge. Pinned by `journey_signals.rs`.
- **Reserved commands gave a generic clap error** — `space leave/delete` said
  "unrecognized subcommand" instead of a clear not-yet-built notice; now
  friendly. Pinned by `journey_reserved.rs`.

## Setup

Build the two real binaries once:

```bash
devbox run -- cargo build -p kutl -p kutl-relay
```

Drive them **exactly as the deterministic harness does** — the harness is the
reference driver. Point the agent at these modules and have it mirror their
model rather than inventing its own:

- `src/harness/relay.rs` — spawns a **real `kutl-relay` subprocess** on an
  ephemeral loopback port (auth is unconditional), pointed at a fresh, initially
  **empty** file-based DID allowlist (`KUTL_RELAY_AUTHORIZED_KEYS_FILE`). The
  file is live-reloaded per auth check; `authorize_did()` appends a DID and it
  takes effect immediately.
- `src/harness/cli.rs` — runs the real `kutl` binary with an isolated
  `KUTL_HOME` (throwaway tempdir) and a per-command `cwd` (space-scoped commands
  resolve the space from the working directory).
- `src/harness/mcp.rs` — drives `kutl mcp serve --agent <agent>` over
  line-delimited JSON-RPC stdio (`initialize` → `tools/list` → `tools/call`).

**Auth model (state this to the agent up front):** the relay requires did:key
auth. Nothing authors until its DID is in the allowlist. Authorize **both** the
human DID (`status --format json` → `identity.did`) and the agent DID before
attempting to raise or triage signals — a missing DID is itself a thing to
judge, but the happy path needs the allowlist populated.

## The naive-user goal (verbatim prompt to hand the agent)

> You are new to kutl. Using only the `kutl` CLI and `kutl mcp serve`, set up a
> workspace, raise a couple of signals, triage them (close / reopen / resolve),
> and inspect the history. You have no prior knowledge of the commands —
> discover them from `--help`. Narrate every confusion, every error message
> that didn't tell you what to do next, and every place a flag or output
> surprised you. When you hit an error, record what you tried, what it said, and
> what you *wished* it had said. Do not read the source to figure out a command
> — if you can't discover it from the CLI itself, that is a finding.

## Judging rubric (what to report — NOT pass/fail)

- **(a) Discoverability** — could the whole goal be reached from `--help` alone,
  with no source-reading? Note every command/flag you needed but couldn't find.
- **(b) Error quality** — does *every* failure name the fix? (e.g. a rejected
  DID should name `authorized_keys` and echo the DID; an unauthenticated triage
  should say how to authenticate.)
- **(c) Output legibility** — are timestamps, status, and ids human-readable and
  **correct**? (A 1970 date, a raw epoch, or an unlabeled uuid is a finding.)
- **(d) Consistency** — do `--format` and shared flags behave uniformly across
  nouns (`space`, `signal`, `document`, `agent`)? Does `--format json` produce
  parseable, uniformly-shaped output everywhere it's offered?
- **(e) Reserved-surface honesty** — do not-yet-built commands (`space
  config/leave/delete`) say so clearly, rather than emitting
  a generic clap error?

## Output — the findings report

A list of findings. Each item:

1. **The confusion** — what happened, verbatim command + output, and why it
   surprised a naive user.
2. **Suggested fix** — the message/flag/output change that would resolve it.
3. **Pin?** — whether it warrants a deterministic pin (a new/extended
   `journey_*.rs` case + a unit test), and roughly where.

No pass/fail verdict — the value is the enumerated confusions, not a green
check.

## How to run it as a Workflow

Trigger this with the multi-agent **Workflow** tool. Two shapes:

- **Single judging agent** — hand it the goal prompt + the rubric + the harness
  references above; it drives the whole surface and returns one report.
- **Fan-out + synthesis** — one agent per surface area (**space**, **signals**,
  **agents**, **mcp**), each judging its slice against the rubric, followed by a
  synthesis agent that dedups and prioritizes into a single report. Use this
  when a run should be thorough; the single-agent shape is fine for a quick
  periodic sweep.

**This is operator-triggered and billed. It is NOT in CI** — only the
deterministic `journey_*.rs` net gates CI. Do not
attempt to wire this pass into any automated runner or hook; it is a periodic,
human-initiated discovery pass.
