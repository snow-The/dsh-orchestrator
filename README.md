# dsh-orchestrator

Ticket-based orchestration for DeepSeek Harness. A minimal, honest slice of the
super-agent design: durable work tickets, a crash-recoverable outbox over the
host's own `ctx.sessionController`, and one structured door through which a
worker reports.

It adds **no daemon, no port, and no second process**.

## Why this exists

The alignment document (`~/.dsh/_p0/ALIGN-superagent-v0.md`) reduces the
super-agent architecture to one testable claim:

> A ticket and an undelivered instruction must both survive a host restart.

That is a property of the *delivery path*, not of a chat loop. This plugin is
the smallest thing that has that property, and it is built so the property is
something the test suite proves rather than something the README asserts.

## The four rules it is built on

1. **One delivery path.** Nothing prompts a worker directly. An instruction is
   appended to the outbox first and delivered by the replay pass — so the code
   that delivers on a quiet afternoon is the code that delivers after a crash.
2. **State is a fold, not a file.** The durable artifact is an append-only
   event log; current state is always `fold(log)`. A killed process can only
   lose the tail it had not acknowledged.
3. **The worker reports through a structured door.** Ticket state is never
   inferred from a worker's prose. `orchestrator_report` is the only way a
   ticket becomes `reported`, and a report whose calling session is not the
   session the ticket is bound to is rejected (`FOREIGN_SESSION`).
4. **No invented host API.** `ctx.sessionController` (`create` / `prompt` /
   `inspect`) and `ctx.tools` are the entire surface. When the controller is
   absent the tools answer `NO_SESSION_CONTROLLER` instead of pretending the
   outbox is empty.

## Tools

| Tool | Purpose |
|---|---|
| `orchestrator_open` | Open a ticket and dispatch it to a fresh worker session. Writes before it sends. |
| `orchestrator_send` | Queue another instruction (`mode: queue`) or interrupt the current turn (`mode: steer`). |
| `orchestrator_report` | Worker files the structured report: `done` / `failed` / `waiting` (+`question`) / `blocked` (+`reason`). |
| `orchestrator_settle` | Close a ticket: `done` / `failed` / `cancelled`. |
| `orchestrator_status` | Tickets that actually need a human, plus what the outbox still owes. |

## State

Everything lives in `<dshHome>/orchestrator/events.jsonl`, one JSON object per
line, `seq` assigned by the log. Ticket state, outbox entries and reports are
all derived from it. `events.jsonl.repair.log` records any repair that had to
happen, rather than hiding it.

## Delivery semantics — what is actually guaranteed

- **At-least-once, with a stable id.** Every outbox entry stores one
  client-minted `requestId` at enqueue time and reuses it for every retry. A
  crash between "the host accepted the prompt" and "the acknowledgement reached
  disk" therefore produces a *retry with the same id*, not a silently distinct
  second instruction.
- **Reconcile before resend.** Replay first asks the gateway whether the
  `requestId` is already present in the session; when it is, the entry is
  acknowledged locally and **nothing is sent**.
- **Exactly-once is not claimed.** The presence probe reads a bounded
  inspection; a miss may be a truncated read rather than proof of absence, so
  the probe returns `present` / `absent` / `unknown` and a miss falls back to
  resending. That keeps the failure mode "an identifiable duplicate" instead of
  "a lost instruction".

## Verification status

Verified here, on this machine:

- **25 tests, all passing.**
  - `test/core.test.mjs` (14) covers the durable core: fold determinism, unknown
    event tolerance, torn-tail repair, parse-failure accounting, monotonic `seq`
    across reloads, crash-then-resend with a stable id, reconcile-instead-of-
    resend, backoff deadlines, foreign-session rejection, structured `waiting`,
    report-once / settle-once, and a restart simulation that drains what the
    dead process owed.
  - `test/plugin.test.mjs` (11) loads the **shipped `dist/index.js`** with a stub
    context, so tool registration, argument plumbing, error mapping and the
    mount-time replay are all the real bundle. It covers: every tool registered
    with an actionable description; open → bind → deliver; a report accepted
    from the bound worker and refused from any other session; `waiting` without
    a question rejected; `status` hiding settled tickets; `send` refusing an
    unknown or settled ticket; **a process that died before delivering being
    drained when the next one mounts** (same `requestId`); a failed delivery not
    retried inside its backoff window; reconcile skipping the resend with a
    `reconciled: true` ack on disk; and `NO_SESSION_CONTROLLER` instead of a
    false "empty outbox".
- `guide_scan` PASS (15 pass / 0 fail).
- `tsc` typecheck clean; both bundles emitted.

**Not yet verified:** this plugin has never been mounted against a live host, so
the live `ctx.sessionController` adapter has only been exercised through a stub.
Two specific unknowns remain until it is mounted:

1. Whether `inspect()` returns enough of the event tail for the presence probe
   to prove presence (a miss degrades to a resend with the same id, which is
   documented above rather than papered over).
2. Whether a restricted worker preset can be named through `agentPreset` in this
   deployment — `create()` accepts it, but no preset has been authored yet.

Mounting is a profile change plus a host restart, which is a deliberate step.

## Install

```bash
dsh plugin add <path-to-this-directory>
```

`dsh plugin add` appends the row to `dsh.profile.bundles`; restart the host to
mount it. `engines.dsh` is `>=0.2.1-rc.2` — a single lower bound, no upper
bound.
