/**
 * dsh-orchestrator — ticket-based orchestration over the host's own session
 * controller.
 *
 * This is the minimal vertical slice the super-agent alignment document calls
 * for, and it is deliberately shaped by four rules:
 *
 *  1. ONE DELIVERY PATH. Nothing prompts a worker directly. Every instruction
 *     becomes an outbox entry first and is delivered by the replay pass, so the
 *     code that delivers on a quiet afternoon is the same code that delivers
 *     after a crash. A second, faster path would be the one that loses work.
 *
 *  2. STATE IS A FOLD, NOT A FILE. The durable artifact is an append-only event
 *     log; "current state" is always derived from it. That is what makes
 *     "survives restart" a property we can test instead of a claim.
 *
 *  3. THE WORKER REPORTS THROUGH A STRUCTURED DOOR. Ticket state can never be
 *     inferred from a worker's prose. `orchestrator_report` is the only way a
 *     ticket becomes `reported`, and it rejects a report whose calling session
 *     is not the one the ticket is bound to.
 *
 *  4. NO INVENTED HOST API. Everything here goes through `ctx.sessionController`
 *     as documented (`create` / `prompt` / `inspect`) and through `ctx.tools`.
 *     When the controller is absent the tools answer a structured failure
 *     instead of pretending there is no work.
 */
import { randomUUID } from 'node:crypto'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { EventLog } from './log.js'
import { liveGateway, type Gateway, type SessionControllerLike } from './gateway.js'
import { replayOutbox } from './replay.js'
import {
  EMPTY_STATE,
  RuleError,
  attention,
  dueInputs,
  fold,
  reduce,
  type OrchestratorState,
  type ReportStatus,
} from './state.js'

export const name = 'orchestrator'
export const inject = ['tools']

interface Tool {
  name: string
  description: string
  parameters: { properties: Record<string, unknown>; required?: string[]; type: 'object' }
  output?: { schema: Record<string, unknown>; render: (args: unknown, value: unknown) => { type: string; text: string }[] }
  execute: (args: Record<string, unknown>, exec: unknown) => Promise<unknown>
  isConcurrencySafe?: () => boolean
}

interface Ctx {
  tools: { register: (tool: Tool) => void }
  get?: (name: string) => unknown
  inject?: (deps: string[], callback: (scope: unknown) => unknown) => unknown
}

const textOutput = (): Tool['output'] => ({
  schema: { type: 'object', additionalProperties: true },
  render: (_args, value) => [{ type: 'text', text: typeof value === 'string' ? value : JSON.stringify(value, null, 2) }],
})

/** The calling session's identity comes from the official handle. */
function sessionIdOf(exec: unknown): string | undefined {
  const agent = (exec as { agent?: { id?: unknown } } | null | undefined)?.agent
  const id = agent?.id
  return typeof id === 'string' && id !== '' ? id : undefined
}

function fail(code: string, message: string, extra: Record<string, unknown> = {}) {
  return { code, message, ok: false as const, ...extra }
}

/** Turn a rule violation into a structured answer instead of an exception. */
function asFailure(error: unknown) {
  if (error instanceof RuleError) return fail(error.code, error.message)
  return fail('INTERNAL', error instanceof Error ? error.message : String(error))
}

/**
 * Read a required string argument.
 *
 * Measured need, not defensive decoration: a caller that wraps its arguments one
 * level too deep yields `undefined` for every field, and `String(undefined)`
 * politely enqueues the literal text "undefined" into the outbox and delivers
 * it to a real worker session. Refusing loudly is the only honest answer.
 */
function requiredArg(args: Record<string, unknown>, key: string): string {
  const value = args[key]
  if (typeof value !== 'string' || value.trim() === '') {
    throw new RuleError('INVALID_ARGS', `"${key}" is required and must be a non-empty string (received ${value === undefined ? 'undefined' : typeof value})`)
  }
  return value
}

export interface OrchestratorOptions {
  /** Overrides `$DSH_HOME` and `~/.dsh`. */
  dshHome?: string
}

export function apply(ctx: Ctx, options: OrchestratorOptions = {}): void {
  const dshHome = options.dshHome ?? process.env.DSH_HOME ?? join(homedir(), '.dsh')
  const log = new EventLog(join(dshHome, 'orchestrator', 'events.jsonl'))
  let state: OrchestratorState = EMPTY_STATE
  let loaded = false
  let replayChain: Promise<unknown> = Promise.resolve()

  /** Reload from disk once; every later read is served from the folded cache. */
  function ready(): OrchestratorState {
    if (!loaded) {
      state = fold(log.load().events)
      loaded = true
    }
    return state
  }

  /** Append one event and advance the cache synchronously, so appends cannot interleave. */
  function commit(type: string, payload: Record<string, unknown>): OrchestratorState {
    const current = ready()
    const event = log.append(type, payload)
    state = reduce(current, event)
    return state
  }

  function controller(): SessionControllerLike | undefined {
    const service = ctx.get?.('sessionController') as SessionControllerLike | undefined
    return service ?? undefined
  }

  function gateway(): Gateway | undefined {
    const service = controller()
    return service ? liveGateway(service) : undefined
  }

  /** Serialize replay passes: two overlapping passes would double-deliver. */
  function replay(): Promise<Record<string, unknown>> {
    const run = replayChain.then(async () => {
      const gw = gateway()
      if (!gw) return fail('NO_SESSION_CONTROLLER', 'ctx.sessionController is unavailable, so the outbox cannot be drained')
      const current = ready()
      const { report, state: next } = await replayOutbox({ log, state: current, gateway: gw, now: Date.now() })
      state = next
      return { ok: true as const, ...report }
    })
    replayChain = run.catch(() => undefined)
    return run as Promise<Record<string, unknown>>
  }

  function summary(target: OrchestratorState) {
    const tickets = Object.values(target.tickets)
    const inputs = Object.values(target.inputs)
    const byState: Record<string, number> = {}
    for (const ticket of tickets) byState[ticket.state] = (byState[ticket.state] ?? 0) + 1
    return {
      tickets: tickets.length,
      tickets_by_state: byState,
      inputs: inputs.length,
      inputs_pending: inputs.filter((entry) => entry.state === 'pending').length,
      inputs_failed: inputs.filter((entry) => entry.state === 'pending' && entry.attempts > 0).length,
    }
  }

  // Record the mount itself. Without this, "the plugin loaded" has no evidence
  // beyond a log line somewhere; with it, a mounted event in the durable log is
  // proof the host composed this plugin and that `apply` ran to completion.
  //
  // `ready()` must come first: the log assigns `seq` from the highest sequence
  // it has loaded, and appending before loading would hand every mount `seq: 1`
  // — duplicate sequence numbers silently break the total order the fold and
  // the restart guarantee both rest on.
  try {
    ready()
    log.append('plugin.mounted', { dshHome, pid: process.pid, version: '0.1.0' })
  } catch {
    // A read-only or missing state directory must not prevent the tools from
    // registering: orchestration without history still beats no orchestration.
  }

  // A replay pass at mount is the whole point: whatever the previous process
  // owed is delivered before anyone has to remember it existed. Mount must not
  // fail because of it, so a rejected pass is swallowed here and surfaces
  // through `orchestrator_status` instead.
  const mountReplay = () => {
    void replay().catch(() => undefined)
  }
  if (ctx.inject) {
    ctx.inject(['sessionController'], () => {
      mountReplay()
    })
  } else {
    mountReplay()
  }

  // ---------------- orchestrator_open ----------------
  ctx.tools.register({
    name: 'orchestrator_open',
    description:
      'Open a work ticket and dispatch it to a fresh worker session. The ticket and its first instruction are written to a durable log before anything is sent, so a crash between writing and sending loses neither. Returns the ticket and what the replay pass actually delivered.',
    parameters: {
      properties: {
        cwd: { description: 'Workspace the worker session owns. Defaults to the host default.', type: 'string' },
        dispatch: { description: 'Create the worker session and send the prompt now. Default true.', type: 'boolean' },
        preset: { description: 'Agent preset the worker session runs, e.g. a restricted worker preset.', type: 'string' },
        prompt: { description: 'The instruction the worker receives. Recorded verbatim except that {{ticket}} is replaced by this ticket id, so the opening instruction can tell the worker which ticket to report against.', type: 'string' },
        title: { description: 'Short human-readable title for the ticket.', type: 'string' },
      },
      required: ['title', 'prompt'],
      type: 'object',
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        // Validate before writing anything or creating a session: a call that
        // cannot be dispatched must not leave a ticket or a worker behind.
        const title = requiredArg(args, 'title')
        const promptText = requiredArg(args, 'prompt')
        const current = ready()
        const ticketId = `t_${randomUUID().slice(0, 8)}`
        commit('ticket.created', {
          ticketId,
          title,
          ...(typeof args.preset === 'string' ? { preset: args.preset } : {}),
          ...(typeof args.cwd === 'string' ? { cwd: args.cwd } : {}),
        })

        if (args.dispatch === false) {
          return { ok: true as const, delivered: null, ticket: state.tickets[ticketId], ticket_before: current.tickets[ticketId] ?? null }
        }

        const gw = gateway()
        if (!gw) return fail('NO_SESSION_CONTROLLER', 'ctx.sessionController is unavailable, so no worker session can be created')

        const created = await gw.createSession({
          ...(typeof args.cwd === 'string' ? { cwd: args.cwd } : {}),
          ...(typeof args.preset === 'string' ? { agentPreset: args.preset } : {}),
        })
        commit('ticket.bound', { ticketId, workerSessionId: created.sessionId })
        const inputId = `i_${randomUUID().slice(0, 8)}`
        // A worker cannot report without knowing which ticket it is working, and
        // the ticket id only exists once the ticket does — yet this is the one
        // call that both creates it and speaks first. `{{ticket}}` closes that
        // loop, so the opening instruction can name its own ticket.
        const text = promptText.replaceAll('{{ticket}}', ticketId)
        commit('input.enqueued', {
          inputId,
          ticketId,
          sessionId: created.sessionId,
          requestId: `req_${randomUUID()}`,
          mode: 'queue',
          text,
        })

        const delivered = await replay()
        return { ok: true as const, delivered, ticket: state.tickets[ticketId] }
      } catch (error) {
        return asFailure(error)
      }
    },
  })

  // ---------------- orchestrator_send ----------------
  ctx.tools.register({
    name: 'orchestrator_send',
    description:
      'Queue one more instruction for an existing ticket\'s worker. Use mode "steer" to interrupt the current turn and "queue" (default) to append. Delivery goes through the same durable outbox as the first instruction.',
    parameters: {
      properties: {
        mode: { description: '"queue" appends after the current turn; "steer" interrupts it. Default queue.', type: 'string' },
        prompt: { description: 'Instruction text, recorded verbatim.', type: 'string' },
        ticket: { description: 'Ticket id, e.g. t_1a2b3c4d.', type: 'string' },
      },
      required: ['ticket', 'prompt'],
      type: 'object',
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready()
        const ticketId = requiredArg(args, 'ticket')
        const ticket = state.tickets[ticketId]
        if (!ticket) return fail('UNKNOWN_TICKET', `no ticket ${ticketId}`)
        if (ticket.state === 'settled') return fail('TICKET_SETTLED', `ticket ${ticketId} is already settled`)
        if (!ticket.workerSessionId) return fail('TICKET_UNBOUND', `ticket ${ticketId} has no worker session`)

        const inputId = `i_${randomUUID().slice(0, 8)}`
        commit('input.enqueued', {
          inputId,
          ticketId,
          sessionId: ticket.workerSessionId,
          requestId: `req_${randomUUID()}`,
          mode: args.mode === 'steer' ? 'steer' : 'queue',
          text: requiredArg(args, 'prompt'),
        })
        const delivered = await replay()
        return { ok: true as const, delivered, input: state.inputs[inputId], ticket: state.tickets[ticketId] }
      } catch (error) {
        return asFailure(error)
      }
    },
  })

  // ---------------- orchestrator_report ----------------
  ctx.tools.register({
    name: 'orchestrator_report',
    description:
      'File the structured report for a ticket you are working. This is the ONLY way a ticket becomes reported: prose in your final message is not read as state. Use status "waiting" with a concrete question when a human decision is required, and "done" / "failed" / "blocked" otherwise.',
    parameters: {
      properties: {
        detail: { description: 'Free-form structured detail for the human. Never interpreted as ticket state.', type: 'object' },
        question: { description: 'Required when status is "waiting": the exact question a human must answer.', type: 'string' },
        reason: { description: 'Required when status is "blocked".', type: 'string' },
        status: { description: 'One of done | failed | waiting | blocked.', type: 'string' },
        summary: { description: 'One or two sentences of what happened.', type: 'string' },
        ticket: { description: 'Ticket id you were given.', type: 'string' },
      },
      required: ['ticket', 'status', 'summary'],
      type: 'object',
    },
    output: textOutput(),
    execute: async (args, exec) => {
      try {
        ready()
        const by = sessionIdOf(exec)
        if (!by) return fail('NO_SESSION_ID', 'this tool call carries no session identity (exec.agent.id is absent)')
        const ticketId = requiredArg(args, 'ticket')
        const report = {
          status: requiredArg(args, 'status') as ReportStatus,
          summary: requiredArg(args, 'summary'),
          ...(typeof args.question === 'string' ? { question: args.question } : {}),
          ...(typeof args.reason === 'string' ? { reason: args.reason } : {}),
          ...(typeof args.detail === 'object' && args.detail !== null ? { detail: args.detail as Record<string, unknown> } : {}),
        }
        commit('ticket.reported', { bySessionId: by, report, ticketId })
        const ticket = state.tickets[ticketId]
        return { ok: true as const, needs_human: ticket.report?.status === 'waiting' || ticket.report?.status === 'blocked', ticket }
      } catch (error) {
        return asFailure(error)
      }
    },
  })

  // ---------------- orchestrator_settle ----------------
  ctx.tools.register({
    name: 'orchestrator_settle',
    description: 'Close a ticket for good: mark it done, failed, or cancelled. Settled tickets leave the attention list and are never dispatched again.',
    parameters: {
      properties: {
        note: { description: 'Optional closing note.', type: 'string' },
        outcome: { description: 'done | failed | cancelled.', type: 'string' },
        ticket: { description: 'Ticket id.', type: 'string' },
      },
      required: ['ticket', 'outcome'],
      type: 'object',
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready()
        const ticketId = requiredArg(args, 'ticket')
        commit('ticket.settled', {
          ticketId,
          outcome: requiredArg(args, 'outcome'),
          ...(typeof args.note === 'string' ? { note: args.note } : {}),
        })
        return { ok: true as const, ticket: state.tickets[ticketId] }
      } catch (error) {
        return asFailure(error)
      }
    },
  })

  // ---------------- orchestrator_status ----------------
  ctx.tools.register({
    name: 'orchestrator_status',
    description:
      'Show the tickets that actually need a human and what the outbox still owes. Pass replay:true to drain the outbox first. Pass all:true to include settled tickets.',
    parameters: {
      properties: {
        all: { description: 'Include settled tickets. Default false.', type: 'boolean' },
        replay: { description: 'Drain the outbox before reporting. Default false.', type: 'boolean' },
      },
      type: 'object',
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready()
        const delivered = args.replay === true ? await replay() : null
        const current = state
        const tickets = args.all === true ? Object.values(current.tickets) : attention(current)
        const pending = Object.values(current.inputs).filter((entry) => entry.state === 'pending')
        const due = dueInputs(current, Date.now())
        return {
          ok: true as const,
          delivered,
          due_now: due.map((entry) => ({ attempts: entry.attempts, id: entry.id, ticket: entry.ticketId })),
          outbox_pending: pending.map((entry) => ({
            attempts: entry.attempts,
            id: entry.id,
            last_error: entry.lastError ?? null,
            not_before: entry.notBefore > 0 ? new Date(entry.notBefore).toISOString() : null,
            ticket: entry.ticketId,
          })),
          summary: summary(current),
          tickets: tickets.map((ticket) => ({
            id: ticket.id,
            outcome: ticket.outcome ?? null,
            question: ticket.report?.question ?? null,
            reported_by: ticket.reportedBy ?? null,
            state: ticket.state,
            status: ticket.report?.status ?? null,
            title: ticket.title,
            worker_session_id: ticket.workerSessionId ?? null,
          })),
        }
      } catch (error) {
        return asFailure(error)
      }
    },
    isConcurrencySafe: () => true,
  })

  // ---------------- orchestrator_reachability ----------------
  /**
   * How many sessions could actually be woken.
   *
   * C10 is the consensus row that says a wake can only reach a session that
   * already owns a live Agent. GAS measured 1 of 333 on their side; this is the
   * same question asked from inside the host that would have to answer it. It is
   * read-only on purpose: `AgentRegistry.resume()` exists, but calling it would
   * make this plugin the owner of somebody else's session (the C9 mistake), so
   * the probe counts and never resumes.
   */
  ctx.tools.register({
    name: 'orchestrator_reachability',
    description:
      'Report how many persisted sessions could actually be woken: the host marks each with agentAvailable, so "reachable" is measured, not guessed. Read-only — it never resumes a session, because resuming would make this plugin the owner of a session it does not own. Use before dispatching work to a cold worker.',
    parameters: {
      properties: {
        sample: { description: 'How many unreachable session ids to name. Default 5.', type: 'number' },
      },
      type: 'object',
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        const controller = ctx.get?.('sessionController') as
          | { list?: (request: Record<string, unknown>, signal: AbortSignal) => Promise<{ items?: readonly Record<string, unknown>[] }> }
          | undefined
        const agents = ctx.get?.('agents') as
          | { get?: (id: string) => unknown; list?: () => readonly unknown[] }
          | undefined

        if (!controller?.list) {
          return fail('NO_SESSION_CONTROLLER', 'ctx.sessionController.list is unavailable, so the session set cannot be read')
        }

        const response = await controller.list({}, new AbortController().signal)
        const items = response?.items ?? []
        const ids = items.map((item) => String(item.sessionId))
        const available = items.filter((item) => item.agentAvailable === true)
        const unreachable = items.filter((item) => item.agentAvailable !== true)
        const sampleSize = typeof args.sample === 'number' && Number.isFinite(args.sample) ? Math.max(0, Math.min(50, args.sample)) : 5

        // Cross-check the flag against the registry itself: the summary is a
        // cached projection, `agents.get` is the thing a wake would actually use.
        const registrySays = typeof agents?.get === 'function'
          ? ids.filter((id) => agents.get?.(id) !== undefined).length
          : null
        const registryList = typeof agents?.list === 'function' ? agents.list().length : null

        return {
          ok: true as const,
          agents_get_agrees: registrySays === null ? null : registrySays === available.length,
          live_agent_count: registryList,
          reachable: available.length,
          reachable_by_registry: registrySays,
          sessions: ids.length,
          unreachable: unreachable.length,
          unreachable_sample: unreachable.slice(0, sampleSize).map((item) => ({
            blank: item.blank === true,
            id: String(item.sessionId),
            origin: typeof item.origin === 'string' ? item.origin : null,
            running: item.running === true,
          })),
          verdict: ids.length === 0
            ? 'no sessions reported'
            : available.length === ids.length
              ? 'every session owns a live agent'
              : 'wake reaches only the sessions that already own a live agent (C10)',
        }
      } catch (error) {
        return asFailure(error)
      }
    },
    isConcurrencySafe: () => true,
  })
}
