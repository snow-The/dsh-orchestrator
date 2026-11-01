import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'

import {
  EventLog,
  RuleError,
  backoffMs,
  attention,
  dueInputs,
  fakeGateway,
  fold,
  replayOutbox,
} from '../dist/core.js'

function tempLog(name = 'events.jsonl') {
  const dir = mkdtempSync(join(tmpdir(), 'orch-'))
  return { dir, file: join(dir, name), log: new EventLog(join(dir, name)) }
}

function codeOf(fn) {
  try {
    fn()
  } catch (error) {
    assert.ok(error instanceof RuleError, `expected RuleError, got ${error?.name}`)
    return error.code
  }
  return null
}

// --- fold ------------------------------------------------------------------

test('fold is order-independent over seq and idempotent for acked inputs', () => {
  const events = [
    { v: 1, seq: 3, at: '2026-01-01T00:00:03.000Z', type: 'input.acked', inputId: 'i1' },
    { v: 1, seq: 1, at: '2026-01-01T00:00:01.000Z', type: 'ticket.created', ticketId: 't1', title: 'fix ci' },
    { v: 1, seq: 2, at: '2026-01-01T00:00:02.000Z', type: 'input.enqueued', inputId: 'i1', ticketId: 't1', sessionId: 'w1', requestId: 'r1', mode: 'queue', text: 'go' },
    { v: 1, seq: 4, at: '2026-01-01T00:00:04.000Z', type: 'input.acked', inputId: 'i1' },
  ]
  const state = fold(events)
  assert.equal(state.tickets.t1.state, 'dispatched')
  assert.equal(state.inputs.i1.state, 'acked')
  assert.equal(fold(events).tickets.t1.state, state.tickets.t1.state)
})

test('an unknown event type is skipped, not fatal', () => {
  const state = fold([
    { v: 1, seq: 1, at: '2026-01-01T00:00:00.000Z', type: 'ticket.created', ticketId: 't1', title: 'x' },
    { v: 1, seq: 2, at: '2026-01-01T00:00:01.000Z', type: 'from.the.future', whatever: true },
  ])
  assert.equal(Object.keys(state.tickets).length, 1)
})

// --- crash durability of the log -------------------------------------------

test('a torn final line is dropped, earlier lines survive, and the file is repaired', () => {
  const { file, log } = tempLog()
  log.append('ticket.created', { ticketId: 't1', title: 'keep me' })
  log.append('ticket.created', { ticketId: 't2', title: 'keep me too' })

  // Simulate a process killed mid-write.
  writeFileSync(file, `${readFileSync(file, 'utf8')}{"v":1,"seq":3,"at":"2026`, 'utf8')

  const reloaded = new EventLog(file)
  const report = reloaded.load()

  assert.equal(report.events.length, 2)
  assert.ok(report.repairedTailBytes > 0)
  assert.equal(report.rewrote, true)
  assert.ok(readFileSync(file, 'utf8').endsWith('\n'), 'repair restores a clean line boundary')

  // The next append must land on its own line and be recoverable.
  reloaded.append('ticket.created', { ticketId: 't3', title: 'after repair' })
  const third = new EventLog(file).load()
  assert.deepEqual(third.events.map((event) => event.ticketId ?? null), ['t1', 't2', 't3'])
  assert.equal(third.repairedTailBytes, 0)
})

test('an unparseable middle line is counted, not silently tolerated', () => {
  const { file } = tempLog()
  const good = '{"v":1,"seq":1,"at":"2026-01-01T00:00:00.000Z","type":"ticket.created","ticketId":"t1","title":"a"}'
  writeFileSync(file, `${good}\nnot json at all\n`, 'utf8')
  const report = new EventLog(file).load()
  assert.equal(report.events.length, 1)
  assert.equal(report.skippedLines, 1)
})

test('sequence numbers stay monotonic across a reload', () => {
  const { file } = tempLog()
  const first = new EventLog(file)
  first.load()
  first.append('ticket.created', { ticketId: 't1', title: 'a' })
  first.append('ticket.created', { ticketId: 't2', title: 'b' })

  const second = new EventLog(file)
  second.load()
  const third = second.append('ticket.created', { ticketId: 't3', title: 'c' })

  assert.equal(third.seq, 3)
})

// --- outbox replay ---------------------------------------------------------

function seeded() {
  const { file, log } = tempLog()
  log.load()
  log.append('ticket.created', { ticketId: 't1', title: 'ship the slice' })
  log.append('ticket.bound', { ticketId: 't1', workerSessionId: 'worker-1' })
  log.append('input.enqueued', {
    inputId: 'i1', ticketId: 't1', sessionId: 'worker-1', requestId: 'req-fixed', mode: 'queue', text: 'do it',
  })
  return { file, log, state: fold(log.load().events) }
}

test('crash after prompt-accept but before ack: replay resends with the same request id', async () => {
  const { log, state } = seeded()
  assert.equal(state.inputs.i1.state, 'pending', 'the ack never reached disk')

  const gateway = fakeGateway({ presence: 'absent' })
  const first = await replayOutbox({ log, state, gateway, now: 10_000 })

  assert.equal(first.report.delivered, 1)
  assert.equal(first.report.reconciled, 0)
  assert.equal(gateway.prompts.length, 1)
  assert.equal(gateway.prompts[0].requestId, 'req-fixed', 'the stable id is what makes the duplicate identifiable')
  assert.equal(first.state.inputs.i1.state, 'acked')

  const second = await replayOutbox({ log, state: first.state, gateway, now: 20_000 })
  assert.equal(second.report.considered, 0)
  assert.equal(gateway.prompts.length, 1, 'an acknowledged entry is never resent')
})

test('replay reconciles instead of resending when the request is already in the session', async () => {
  const { log, state } = seeded()
  const gateway = fakeGateway({ presence: 'present' })

  const result = await replayOutbox({ log, state, gateway, now: 10_000 })

  assert.equal(gateway.prompts.length, 0, 'nothing is sent when the probe proves presence')
  assert.equal(result.report.reconciled, 1)
  assert.equal(result.state.inputs.i1.state, 'acked')
})

test('a failed delivery is backed off and retried only after the deadline', async () => {
  const { log, state } = seeded()
  const gateway = fakeGateway({ failWith: new Error('transport down') })

  const first = await replayOutbox({ log, state, gateway, now: 10_000 })
  assert.equal(first.report.deferred, 1)
  assert.equal(first.report.remaining, 1)
  assert.equal(first.state.inputs.i1.state, 'pending')
  assert.equal(first.state.inputs.i1.attempts, 1)
  assert.equal(first.state.inputs.i1.lastError, 'transport down')
  assert.equal(first.state.inputs.i1.notBefore, 10_000 + backoffMs(1))

  const tooEarly = await replayOutbox({ log, state: first.state, gateway, now: 10_500 })
  assert.equal(tooEarly.report.considered, 0, 'backoff is respected')

  gateway.failWith = undefined
  const later = await replayOutbox({ log, state: first.state, gateway, now: 30_000 })
  assert.equal(later.report.delivered, 1)
  assert.equal(gateway.prompts[0].requestId, 'req-fixed', 'the retry reuses the original id, it does not mint a new one')
})

test('dueInputs ignores acked entries and orders by creation', () => {
  const { log } = seeded()
  log.append('input.enqueued', {
    inputId: 'i2', ticketId: 't1', sessionId: 'worker-1', requestId: 'req-2', mode: 'steer', text: 'second',
  })
  log.append('input.acked', { inputId: 'i2' })
  const state = fold(log.load().events)
  const due = dueInputs(state, 10_000)
  assert.deepEqual(due.map((entry) => entry.id), ['i1'])
})

// --- live gateway contract -------------------------------------------------

test('the live gateway always hands the host an AbortSignal', async () => {
  const { liveGateway } = await import('../dist/core.js')
  const seen = []
  const controller = {
    async create() {
      return { sessionId: 'w1' }
    },
    async prompt(request, signal) {
      // The host implementation calls `signal.throwIfAborted()` and fails the
      // delivery outright when it is missing, so this is a real contract.
      seen.push({ hasSignal: signal !== undefined && typeof signal.throwIfAborted === 'function', request })
      signal.throwIfAborted()
      return { accepted: true }
    },
    async inspect() {
      return {}
    },
  }
  const gateway = liveGateway(controller)
  const result = await gateway.prompt({ mode: 'queue', requestId: 'req-1', sessionId: 'w1', text: 'hello' })

  assert.equal(result.accepted, true)
  assert.equal(seen.length, 1)
  assert.equal(seen[0].hasSignal, true, 'without a signal the host throws instead of delivering')
})

test('the live gateway refuses to treat a non-accepting host as success', async () => {
  const { liveGateway } = await import('../dist/core.js')
  const gateway = liveGateway({
    async create() {
      return { sessionId: 'w1' }
    },
    async prompt() {
      return { accepted: false }
    },
    async inspect() {
      return {}
    },
  })
  await assert.rejects(
    () => gateway.prompt({ mode: 'queue', requestId: 'req-1', sessionId: 'w1', text: 'x' }),
    /did not acknowledge/,
  )
})

// --- report rules ----------------------------------------------------------

test('a report from a session other than the bound worker is rejected', () => {
  const { log } = seeded()
  const state = fold(log.load().events)
  const code = codeOf(() => fold([
    ...log.load().events,
    {
      v: 1, seq: 99, at: '2026-01-01T00:00:09.000Z', type: 'ticket.reported',
      ticketId: 't1', bySessionId: 'someone-else',
      report: { status: 'done', summary: 'trust me' },
    },
  ]))
  assert.equal(state.tickets.t1.workerSessionId, 'worker-1')
  assert.equal(code, 'FOREIGN_SESSION')
})

test('waiting requires a structured question; narrative text never sets it', () => {
  const { log } = seeded()
  const base = log.load().events
  const at = '2026-01-01T00:00:09.000Z'

  const noQuestion = codeOf(() => fold([...base, {
    v: 1, seq: 90, at, type: 'ticket.reported', ticketId: 't1', bySessionId: 'worker-1',
    report: { status: 'waiting', summary: 'I have a question about the schema' },
  }]))
  assert.equal(noQuestion, 'INVALID_REPORT')

  const state = fold([...base, {
    v: 1, seq: 91, at, type: 'ticket.reported', ticketId: 't1', bySessionId: 'worker-1',
    report: { status: 'waiting', summary: 'blocked on a decision', question: 'Migrate or dual-write?' },
  }])
  assert.equal(state.tickets.t1.state, 'reported')
  assert.equal(state.tickets.t1.report.status, 'waiting')
  assert.equal(attention(state).length, 1, 'a waiting ticket needs a human')
})

test('a ticket may be reported once and settled once', () => {
  const { log } = seeded()
  const base = log.load().events
  const at = '2026-01-01T00:00:09.000Z'
  const report = {
    v: 1, seq: 90, at, type: 'ticket.reported', ticketId: 't1', bySessionId: 'worker-1',
    report: { status: 'done', summary: 'shipped' },
  }
  const state = fold([...base, report])
  assert.equal(state.tickets.t1.state, 'reported')

  assert.equal(codeOf(() => fold([...base, report, { ...report, seq: 91 }])), 'ALREADY_REPORTED')

  const settled = fold([...base, report, { v: 1, seq: 92, at, type: 'ticket.settled', ticketId: 't1', outcome: 'done' }])
  assert.equal(settled.tickets.t1.state, 'settled')
  assert.equal(attention(settled).length, 0, 'settled tickets leave the attention list')

  assert.equal(codeOf(() => fold([
    ...base, report,
    { v: 1, seq: 92, at, type: 'ticket.settled', ticketId: 't1', outcome: 'done' },
    { v: 1, seq: 93, at, type: 'ticket.settled', ticketId: 't1', outcome: 'done' },
  ])), 'TICKET_SETTLED')
})

test('a resolved ("done") report does not demand attention', () => {
  const { log } = seeded()
  const state = fold([...log.load().events, {
    v: 1, seq: 90, at: '2026-01-01T00:00:09.000Z', type: 'ticket.reported', ticketId: 't1', bySessionId: 'worker-1',
    report: { status: 'done', summary: 'shipped' },
  }])
  assert.equal(attention(state).length, 0)
})

// --- full restart simulation ----------------------------------------------

test('ticket state and undelivered input both survive a restart', async () => {
  const { file, log, state } = seeded()

  // "Restart": a brand-new log instance over the same file, no in-memory carry-over.
  const afterRestart = new EventLog(file)
  const loaded = afterRestart.load()
  const recovered = fold(loaded.events)

  assert.equal(loaded.repairedTailBytes, 0)
  assert.equal(recovered.tickets.t1.title, 'ship the slice')
  assert.equal(recovered.tickets.t1.workerSessionId, 'worker-1')
  assert.equal(Object.values(recovered.inputs).filter((entry) => entry.state === 'pending').length, 1)

  // The recovered process can still deliver what the dead one owed.
  const gateway = fakeGateway({ presence: 'absent' })
  const drained = await replayOutbox({ log: afterRestart, state: recovered, gateway, now: 1 })
  assert.equal(drained.report.delivered, 1)
  assert.equal(gateway.prompts[0].text, 'do it')
  assert.equal(fold(afterRestart.load().events).inputs.i1.state, 'acked')
})
