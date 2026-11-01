/**
 * Exercises the shipped bundle, not the source: `dist/index.js` is loaded with a
 * stub context so tool registration, argument plumbing, error mapping and the
 * mount-time replay are all the real thing. A live host would additionally prove
 * the controller adapter; that step needs a profile change plus a restart, so it
 * stays an explicit action rather than something a test fakes away.
 */
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'

const { apply } = await import('../dist/index.js')

function harness(options = {}) {
  const dshHome = options.dshHome ?? mkdtempSync(join(tmpdir(), 'orch-mount-'))
  const tools = new Map()
  const calls = { create: [], inspect: [], prompt: [] }
  let failPrompt = options.failPrompt ?? false
  let presence = options.presence ?? 'absent'
  let counter = 0

  const controller = {
    async create(request) {
      calls.create.push(request)
      counter += 1
      return { sessionId: `worker-${counter}`, ...(request.agentPreset ? { agentPreset: request.agentPreset } : {}) }
    },
    async inspect(sessionId) {
      calls.inspect.push(sessionId)
      return presence === 'present' ? { sessionId, requestIds: calls.prompt.map((call) => call.requestId) } : { sessionId }
    },
    async prompt(request) {
      if (failPrompt) throw new Error('host rejected the prompt')
      calls.prompt.push(request)
      return { accepted: true }
    },
    // The reachability probe reads the host's own summary flag, which is a
    // cached projection, and cross-checks it against the registry.
    async list() {
      return { items: options.sessions ?? [] }
    },
  }

  const liveAgents = new Set(options.liveAgents ?? [])
  const agents = {
    get: (id) => (liveAgents.has(id) ? { id } : undefined),
    list: () => [...liveAgents].map((id) => ({ id })),
  }

  const ctx = {
    tools: { register: (tool) => tools.set(tool.name, tool) },
    get: (name) => {
      if (name === 'sessionController') return controller
      if (name === 'agents') return agents
      return undefined
    },
  }
  apply(ctx, { dshHome })

  return {
    calls,
    controller,
    ctx,
    dshHome,
    tools,
    setFailPrompt: (value) => { failPrompt = value },
    setPresence: (value) => { presence = value },
  }
}

const lead = { agent: { id: 'lead-session' } }
const call = (tools, name, args, exec = lead) => tools.get(name).execute(args, exec)

test('missing arguments are refused instead of enqueuing the literal "undefined"', async () => {
  const h = harness()
  // The exact shape of the mistake that happened live: caller wraps its args one
  // level too deep, so every field reads as undefined.
  const wrapped = await call(h.tools, 'orchestrator_open', { params: { prompt: 'x', title: 'y' } })
  assert.equal(wrapped.code, 'INVALID_ARGS')
  assert.match(wrapped.message, /title/)
  assert.equal(h.calls.prompt.length, 0, 'nothing is delivered')

  const missingPrompt = await call(h.tools, 'orchestrator_open', { title: 'only a title' })
  assert.equal(missingPrompt.code, 'INVALID_ARGS')
  assert.match(missingPrompt.message, /prompt/)
  assert.equal(h.calls.create.length, 0, 'no worker session is created for a call that cannot be dispatched')
})

test('settling a ticket cancels what it still owes, so a dead ticket stops retrying', async () => {
  const core = await import('../dist/core.js')
  const h = harness({ failPrompt: true })
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'owed', title: 't' })
  const ticket = opened.ticket.id
  assert.equal(opened.delivered.remaining, 1)

  await call(h.tools, 'orchestrator_settle', { outcome: 'cancelled', note: 'abandoned', ticket })

  const state = core.fold(new core.EventLog(join(h.dshHome, 'orchestrator', 'events.jsonl')).load().events)
  const owed = Object.values(state.inputs).filter((entry) => entry.ticketId === ticket)
  assert.equal(owed.length, 1)
  assert.equal(owed[0].state, 'cancelled')
  assert.equal(core.dueInputs(state, Date.now() + 10 * 60_000).length, 0, 'a cancelled entry is never due again')
})

test('successive mounts never reuse a sequence number', async () => {
  const core = await import('../dist/core.js')
  const first = harness()
  await call(first.tools, 'orchestrator_open', { dispatch: false, prompt: 'noop', title: 'a' })

  // A second process over the same state directory: the mounted event must take
  // the next sequence, not restart from 1.
  harness({ dshHome: first.dshHome })

  const events = new core.EventLog(join(first.dshHome, 'orchestrator', 'events.jsonl')).load().events
  const seqs = events.map((event) => event.seq)
  assert.equal(new Set(seqs).size, seqs.length, `sequence numbers must be unique, got ${JSON.stringify(seqs)}`)
  assert.equal(events.filter((event) => event.type === 'plugin.mounted').length, 2)
})

test('mounting records itself, so "did the host compose this plugin" is evidence, not a guess', async () => {
  const core = await import('../dist/core.js')
  const h = harness()
  const events = new core.EventLog(join(h.dshHome, 'orchestrator', 'events.jsonl')).load().events
  const mounted = events.filter((event) => event.type === 'plugin.mounted')
  assert.equal(mounted.length, 1)
  assert.equal(mounted[0].version, '0.1.0')
  assert.equal(typeof mounted[0].pid, 'number')
})

test('every tool the plugin promises is registered', () => {
  const { tools } = harness()
  assert.deepEqual([...tools.keys()].sort(), [
    'orchestrator_open',
    'orchestrator_reachability',
    'orchestrator_report',
    'orchestrator_send',
    'orchestrator_settle',
    'orchestrator_status',
  ])
  for (const tool of tools.values()) {
    assert.equal(typeof tool.execute, 'function')
    assert.equal(tool.parameters.type, 'object')
    assert.ok(tool.description.length > 20, `${tool.name} needs a description a model can act on`)
  }
})

test('open writes the ticket, creates the worker, and delivers through the outbox', async () => {
  const h = harness()
  const result = await call(h.tools, 'orchestrator_open', { preset: 'worker', prompt: 'fix the flaky test', title: 'flaky test' })

  assert.equal(result.ok, true)
  assert.equal(result.ticket.state, 'dispatched')
  assert.equal(result.ticket.workerSessionId, 'worker-1')
  assert.equal(h.calls.create.length, 1)
  assert.equal(h.calls.create[0].agentPreset, 'worker')

  // The instruction went out through the controller, wrapped as a text part.
  assert.equal(h.calls.prompt.length, 1)
  assert.deepEqual(h.calls.prompt[0].content, [{ type: 'text', text: 'fix the flaky test' }])
  assert.match(h.calls.prompt[0].requestId, /^req_/)
  assert.equal(result.delivered.delivered, 1)
})

test('a report from the bound worker is accepted; from any other session it is refused', async () => {
  const h = harness()
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'go', title: 't' })
  const ticket = opened.ticket.id
  const worker = { agent: { id: 'worker-1' } }

  const foreign = await call(h.tools, 'orchestrator_report', { status: 'done', summary: 'trust me', ticket }, lead)
  assert.equal(foreign.ok, false)
  assert.equal(foreign.code, 'FOREIGN_SESSION')

  const accepted = await call(h.tools, 'orchestrator_report', { status: 'done', summary: 'shipped', ticket }, worker)
  assert.equal(accepted.ok, true)
  assert.equal(accepted.needs_human, false)
  assert.equal(accepted.ticket.state, 'reported')
})

test('a waiting report demands a question and surfaces in status', async () => {
  const h = harness()
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'go', title: 't' })
  const worker = { agent: { id: 'worker-1' } }

  const vague = await call(h.tools, 'orchestrator_report', { status: 'waiting', summary: 'unsure', ticket: opened.ticket.id }, worker)
  assert.equal(vague.code, 'INVALID_REPORT')

  const asked = await call(h.tools, 'orchestrator_report', {
    question: 'Migrate or dual-write?', status: 'waiting', summary: 'needs a decision', ticket: opened.ticket.id,
  }, worker)
  assert.equal(asked.needs_human, true)

  const status = await call(h.tools, 'orchestrator_status', {})
  assert.equal(status.tickets.length, 1)
  assert.equal(status.tickets[0].question, 'Migrate or dual-write?')
  assert.equal(status.tickets[0].status, 'waiting')
})

test('status hides settled tickets unless asked for them', async () => {
  const h = harness()
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'go', title: 't' })
  await call(h.tools, 'orchestrator_settle', { outcome: 'done', ticket: opened.ticket.id })

  assert.equal((await call(h.tools, 'orchestrator_status', {})).tickets.length, 0)
  const all = await call(h.tools, 'orchestrator_status', { all: true })
  assert.equal(all.tickets.length, 1)
  assert.equal(all.tickets[0].outcome, 'done')
})

test('send appends to the bound worker and refuses an unknown or settled ticket', async () => {
  const h = harness()
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'first', title: 't' })
  const ticket = opened.ticket.id

  const second = await call(h.tools, 'orchestrator_send', { mode: 'steer', prompt: 'second', ticket })
  assert.equal(second.ok, true)
  assert.equal(h.calls.prompt.at(-1).mode, 'steer')
  assert.equal(h.calls.prompt.at(-1).sessionId, 'worker-1')

  assert.equal((await call(h.tools, 'orchestrator_send', { prompt: 'x', ticket: 't_missing' })).code, 'UNKNOWN_TICKET')

  await call(h.tools, 'orchestrator_settle', { outcome: 'cancelled', ticket })
  assert.equal((await call(h.tools, 'orchestrator_send', { prompt: 'late', ticket })).code, 'TICKET_SETTLED')
})

/** Leave on disk exactly what a process that died before delivering would leave. */
async function crashBeforeDelivery(dshHome, prompt) {
  const core = await import('../dist/core.js')
  const log = new core.EventLog(join(dshHome, 'orchestrator', 'events.jsonl'))
  log.load()
  log.append('ticket.created', { ticketId: 't_crash', title: 'unfinished work' })
  log.append('ticket.bound', { ticketId: 't_crash', workerSessionId: 'worker-crash' })
  log.append('input.enqueued', {
    inputId: 'i_crash',
    mode: 'queue',
    requestId: 'req_crash_fixed',
    sessionId: 'worker-crash',
    text: prompt,
    ticketId: 't_crash',
  })
  return 'req_crash_fixed'
}

test('a process that died before delivering is drained when the next one mounts', async () => {
  const dshHome = mkdtempSync(join(tmpdir(), 'orch-crash-'))
  const requestId = await crashBeforeDelivery(dshHome, 'survive this')

  const restarted = harness({ dshHome })
  const status = await call(restarted.tools, 'orchestrator_status', { replay: true })

  assert.equal(restarted.calls.prompt.length, 1, 'the owed instruction is delivered by the recovered process')
  assert.equal(restarted.calls.prompt[0].content[0].text, 'survive this')
  assert.equal(restarted.calls.prompt[0].requestId, requestId, 'the retry reuses the id minted before the crash')
  assert.equal(status.summary.inputs_pending, 0)
  assert.equal(status.tickets[0].id, 't_crash')
  assert.equal(status.tickets[0].state, 'dispatched')
})

test('the opening instruction can name its own ticket through {{ticket}}', async () => {
  const h = harness()
  const result = await call(h.tools, 'orchestrator_open', {
    prompt: 'Report against ticket {{ticket}} when done.',
    title: 'self-reference',
  })

  const sent = h.calls.prompt[0].content[0].text
  assert.equal(sent, `Report against ticket ${result.ticket.id} when done.`)
  assert.ok(!sent.includes('{{ticket}}'), 'the placeholder never reaches the worker raw')
})

test('a failed delivery is not retried before its backoff deadline', async () => {
  const h = harness({ failPrompt: true })
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'back off', title: 't' })
  assert.equal(opened.delivered.deferred, 1)

  // A fresh process, 14ms later, must respect the deadline the failure recorded.
  const restarted = harness({ dshHome: h.dshHome })
  const status = await call(restarted.tools, 'orchestrator_status', { replay: true })

  assert.equal(restarted.calls.prompt.length, 0, 'resending inside the backoff window would defeat the point of recording it')
  assert.equal(status.summary.inputs_pending, 1)
  assert.equal(status.outbox_pending[0].attempts, 1)
  assert.equal(status.outbox_pending[0].last_error, 'host rejected the prompt')
})

test('reconcile skips the resend when the session already has the request', async () => {
  const dshHome = mkdtempSync(join(tmpdir(), 'orch-recon-'))
  const requestId = await crashBeforeDelivery(dshHome, 'already there')

  const h = harness({ dshHome, presence: 'present' })
  // Make the probe able to see the id, as a real session log would.
  h.controller.inspect = async (sessionId) => ({ requestIds: [requestId], sessionId })
  // The mount pass is the one that drains, so the durable record is the evidence.
  const status = await call(h.tools, 'orchestrator_status', { replay: true })

  assert.equal(h.calls.prompt.length, 0, 'nothing is resent when presence is proven')
  assert.equal(status.summary.inputs_pending, 0, 'the entry is settled')

  const core = await import('../dist/core.js')
  const acked = new core.EventLog(join(dshHome, 'orchestrator', 'events.jsonl')).load().events
    .filter((event) => event.type === 'input.acked')
  assert.equal(acked.length, 1)
  assert.equal(acked[0].reconciled, true, 'the ack records that it was reconciled, not sent')
})

test('without a session controller the tools fail explicitly rather than reporting an empty outbox', async () => {
  const dshHome = mkdtempSync(join(tmpdir(), 'orch-noc-'))
  const tools = new Map()
  apply({ tools: { register: (tool) => tools.set(tool.name, tool) }, get: () => undefined }, { dshHome })

  const opened = await call(tools, 'orchestrator_open', { prompt: 'go', title: 't' })
  assert.equal(opened.ok, false)
  assert.equal(opened.code, 'NO_SESSION_CONTROLLER')

  const status = await call(tools, 'orchestrator_status', { replay: true })
  assert.equal(status.delivered.code, 'NO_SESSION_CONTROLLER')
})

test('a report without a caller session identity is refused', async () => {
  const h = harness()
  const opened = await call(h.tools, 'orchestrator_open', { prompt: 'go', title: 't' })
  const result = await call(h.tools, 'orchestrator_report', { status: 'done', summary: 'x', ticket: opened.ticket.id }, {})
  assert.equal(result.code, 'NO_SESSION_ID')
})

test('reachability counts what the host itself reports, and cross-checks the registry', async () => {
  const h = harness({
    liveAgents: ['s-live'],
    sessions: [
      { agentAvailable: true, sessionId: 's-live', running: true, blank: false, updatedAt: 1 },
      { agentAvailable: false, sessionId: 's-cold-1', running: false, blank: false, updatedAt: 2 },
      { agentAvailable: false, sessionId: 's-cold-2', running: false, blank: true, updatedAt: 3 },
    ],
  })

  const report = await call(h.tools, 'orchestrator_reachability', { sample: 2 })

  assert.equal(report.ok, true)
  assert.equal(report.sessions, 3)
  assert.equal(report.reachable, 1)
  assert.equal(report.unreachable, 2)
  assert.equal(report.reachable_by_registry, 1)
  assert.equal(report.agents_get_agrees, true, 'the cached flag and the registry must tell the same story')
  assert.deepEqual(report.unreachable_sample.map((entry) => entry.id), ['s-cold-1', 's-cold-2'])
  assert.match(report.verdict, /only the sessions that already own a live agent/)
})

test('reachability says so plainly when every session is live', async () => {
  const h = harness({
    liveAgents: ['a', 'b'],
    sessions: [
      { agentAvailable: true, sessionId: 'a', running: false, blank: false, updatedAt: 1 },
      { agentAvailable: true, sessionId: 'b', running: false, blank: false, updatedAt: 2 },
    ],
  })
  const report = await call(h.tools, 'orchestrator_reachability', {})
  assert.equal(report.reachable, 2)
  assert.equal(report.verdict, 'every session owns a live agent')
})

test('reachability reports disagreement instead of hiding it', async () => {
  // Summary claims a live agent the registry does not have: a stale projection is
  // exactly the kind of quiet lie worth surfacing rather than smoothing over.
  const h = harness({
    liveAgents: [],
    sessions: [{ agentAvailable: true, sessionId: 'ghost', running: true, blank: false, updatedAt: 1 }],
  })
  const report = await call(h.tools, 'orchestrator_reachability', {})
  assert.equal(report.reachable, 1)
  assert.equal(report.reachable_by_registry, 0)
  assert.equal(report.agents_get_agrees, false)
})
