import type { LogEvent } from './log.js'

/** Terminal or in-flight lifecycle of one work ticket. */
export type TicketState = 'created' | 'dispatched' | 'reported' | 'settled'

/** The only statuses a worker may assert about its own work. */
export type ReportStatus = 'done' | 'failed' | 'waiting' | 'blocked'

/** Delivery state of one outbox entry. */
export type InputState = 'pending' | 'acked' | 'failed' | 'cancelled'

/** Structured report a worker files against its own ticket. */
export interface TicketReport {
  readonly status: ReportStatus
  readonly summary: string
  /** Required when `status` is `waiting`: the exact question a human must answer. */
  readonly question?: string
  /** Required when `status` is `blocked`. */
  readonly reason?: string
  /** Free-form detail the worker chooses to hand back; never read as state. */
  readonly detail?: Record<string, unknown>
}

export interface Ticket {
  readonly id: string
  readonly title: string
  readonly preset?: string
  readonly cwd?: string
  readonly createdAt: string
  /** Worker session this ticket is bound to, once dispatch created or named one. */
  readonly workerSessionId?: string
  readonly state: TicketState
  readonly report?: TicketReport
  readonly reportedBy?: string
  readonly outcome?: 'done' | 'failed' | 'cancelled'
  readonly note?: string
}

export interface OutboxEntry {
  readonly id: string
  readonly ticketId: string
  readonly sessionId: string
  /**
   * Client-minted identity reused across every retry of this entry. It is what
   * makes replay reconcile instead of resend blind.
   */
  readonly requestId: string
  readonly mode: 'queue' | 'steer'
  readonly text: string
  readonly createdAt: string
  readonly state: InputState
  readonly attempts: number
  /** Epoch milliseconds before which replay must not retry a failed entry. */
  readonly notBefore: number
  readonly lastError?: string
}

export interface OrchestratorState {
  readonly tickets: Readonly<Record<string, Ticket>>
  readonly inputs: Readonly<Record<string, OutboxEntry>>
}

export const EMPTY_STATE: OrchestratorState = { tickets: {}, inputs: {} }

/** A rule violation that is the caller's fault, carrying a stable code. */
export class RuleError extends Error {
  constructor(readonly code: string, message: string) {
    super(message)
    this.name = 'RuleError'
  }
}

const RETRY_BASE_MS = 1_000
const RETRY_MAX_MS = 60_000

/** Exponential backoff with a ceiling, kept pure so tests pin it exactly. */
export function backoffMs(attempts: number): number {
  const exponent = Math.max(0, Math.min(16, attempts - 1))
  return Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** exponent)
}

function requireString(payload: Record<string, unknown>, key: string, event: string): string {
  const value = payload[key]
  if (typeof value !== 'string' || value.trim() === '') {
    throw new RuleError('INVALID_EVENT', `${event} requires a non-empty "${key}"`)
  }
  return value
}

/**
 * Validate a report before it can become ticket state.
 *
 * This is the rule the prototype's rejected-design table is really about: a
 * ticket may only be `waiting` because the worker said so in a structured
 * field, never because some sentence in its output looked like a question.
 */
export function validateReport(report: TicketReport): TicketReport {
  if (!['done', 'failed', 'waiting', 'blocked'].includes(report.status)) {
    throw new RuleError('INVALID_REPORT', `unknown report status: ${String(report.status)}`)
  }
  if (typeof report.summary !== 'string' || report.summary.trim() === '') {
    throw new RuleError('INVALID_REPORT', 'report requires a non-empty summary')
  }
  if (report.status === 'waiting' && (typeof report.question !== 'string' || report.question.trim() === '')) {
    throw new RuleError('INVALID_REPORT', 'status "waiting" requires a non-empty question')
  }
  if (report.status === 'blocked' && (typeof report.reason !== 'string' || report.reason.trim() === '')) {
    throw new RuleError('INVALID_REPORT', 'status "blocked" requires a non-empty reason')
  }
  return report
}

/** Apply one durable event. Pure: same state + same event always yields the same result. */
export function reduce(state: OrchestratorState, event: LogEvent): OrchestratorState {
  const payload = event as unknown as Record<string, unknown>

  switch (event.type) {
    case 'ticket.created': {
      const id = requireString(payload, 'ticketId', event.type)
      if (state.tickets[id]) throw new RuleError('TICKET_EXISTS', `ticket ${id} already exists`)
      const ticket: Ticket = {
        id,
        title: requireString(payload, 'title', event.type),
        state: 'created',
        createdAt: event.at,
        ...(typeof payload.preset === 'string' ? { preset: payload.preset } : {}),
        ...(typeof payload.cwd === 'string' ? { cwd: payload.cwd } : {}),
      }
      return { ...state, tickets: { ...state.tickets, [id]: ticket } }
    }

    case 'ticket.bound': {
      const id = requireString(payload, 'ticketId', event.type)
      const ticket = state.tickets[id]
      if (!ticket) throw new RuleError('UNKNOWN_TICKET', `unknown ticket ${id}`)
      const sessionId = requireString(payload, 'workerSessionId', event.type)
      return {
        ...state,
        tickets: { ...state.tickets, [id]: { ...ticket, workerSessionId: sessionId } },
      }
    }

    case 'input.enqueued': {
      const id = requireString(payload, 'inputId', event.type)
      if (state.inputs[id]) throw new RuleError('INPUT_EXISTS', `input ${id} already exists`)
      const ticketId = requireString(payload, 'ticketId', event.type)
      const ticket = state.tickets[ticketId]
      if (!ticket) throw new RuleError('UNKNOWN_TICKET', `unknown ticket ${ticketId}`)
      const entry: OutboxEntry = {
        id,
        ticketId,
        sessionId: requireString(payload, 'sessionId', event.type),
        requestId: requireString(payload, 'requestId', event.type),
        mode: payload.mode === 'steer' ? 'steer' : 'queue',
        text: requireString(payload, 'text', event.type),
        createdAt: event.at,
        state: 'pending',
        attempts: 0,
        notBefore: 0,
      }
      return { ...state, inputs: { ...state.inputs, [id]: entry } }
    }

    case 'input.acked': {
      const id = requireString(payload, 'inputId', event.type)
      const entry = state.inputs[id]
      if (!entry) throw new RuleError('UNKNOWN_INPUT', `unknown input ${id}`)
      if (entry.state === 'acked') return state
      const inputs = { ...state.inputs, [id]: { ...entry, state: 'acked' as const } }
      const ticket = state.tickets[entry.ticketId]
      const dispatched: Ticket = ticket && ticket.state === 'created'
        ? { ...ticket, state: 'dispatched' }
        : ticket as Ticket
      return {
        inputs,
        tickets: ticket ? { ...state.tickets, [entry.ticketId]: dispatched } : state.tickets,
      }
    }

    case 'input.failed': {
      const id = requireString(payload, 'inputId', event.type)
      const entry = state.inputs[id]
      if (!entry) throw new RuleError('UNKNOWN_INPUT', `unknown input ${id}`)
      // An ack that arrived before the failure record always wins, and a
      // cancelled instruction must not come back to life just because a stale
      // failure record for it landed late.
      if (entry.state === 'acked' || entry.state === 'cancelled') return state
      const attempts = entry.attempts + 1
      const error = typeof payload.error === 'string' ? payload.error : 'unknown error'
      // The deadline is computed by whoever decided to retry (the replay pass,
      // which owns the clock it filters by) and merely recorded here. Deriving
      // it from the event timestamp instead would make two different clocks
      // disagree whenever a caller passes an explicit `now`.
      const notBefore = typeof payload.notBefore === 'number' && Number.isFinite(payload.notBefore)
        ? payload.notBefore
        : Date.parse(event.at) + backoffMs(attempts)
      const inputs = {
        ...state.inputs,
        [id]: {
          ...entry,
          state: 'pending' as const,
          attempts,
          notBefore,
          lastError: error,
        },
      }
      return { ...state, inputs }
    }

    case 'ticket.reported': {
      const id = requireString(payload, 'ticketId', event.type)
      const ticket = state.tickets[id]
      if (!ticket) throw new RuleError('UNKNOWN_TICKET', `unknown ticket ${id}`)
      if (ticket.state === 'settled') throw new RuleError('TICKET_SETTLED', `ticket ${id} is already settled`)
      if (ticket.report) throw new RuleError('ALREADY_REPORTED', `ticket ${id} already has a report`)
      const by = requireString(payload, 'bySessionId', event.type)
      if (!ticket.workerSessionId) {
        throw new RuleError('TICKET_UNBOUND', `ticket ${id} has no worker session to report from`)
      }
      if (by !== ticket.workerSessionId) {
        throw new RuleError('FOREIGN_SESSION', `ticket ${id} is bound to ${ticket.workerSessionId}, not ${by}`)
      }
      const report = validateReport(payload.report as TicketReport)
      return {
        ...state,
        tickets: { ...state.tickets, [id]: { ...ticket, state: 'reported', report, reportedBy: by } },
      }
    }

    case 'ticket.settled': {
      const id = requireString(payload, 'ticketId', event.type)
      const ticket = state.tickets[id]
      if (!ticket) throw new RuleError('UNKNOWN_TICKET', `unknown ticket ${id}`)
      if (ticket.state === 'settled') throw new RuleError('TICKET_SETTLED', `ticket ${id} is already settled`)
      const outcome = payload.outcome
      if (outcome !== 'done' && outcome !== 'failed' && outcome !== 'cancelled') {
        throw new RuleError('INVALID_EVENT', `ticket.settled requires outcome done|failed|cancelled, got ${String(outcome)}`)
      }
      // Closing a ticket also closes what it still owes. Leaving those entries
      // pending would make a dead ticket keep retrying forever: the outbox would
      // be delivering instructions for work nobody is waiting for any more.
      //
      // The loop variable must not be called `id`: it would shadow the ticket id
      // above and compare a ticket id against an input id, which is never equal.
      const inputs: Record<string, OutboxEntry> = { ...state.inputs }
      for (const [inputId, entry] of Object.entries(state.inputs)) {
        if (entry.ticketId === id && entry.state === 'pending') {
          inputs[inputId] = { ...entry, state: 'cancelled' }
        }
      }
      return {
        inputs,
        tickets: {
          ...state.tickets,
          [id]: {
            ...ticket,
            state: 'settled',
            outcome,
            ...(typeof payload.note === 'string' ? { note: payload.note } : {}),
          },
        },
      }
    }

    default:
      // An unknown event type is data we do not understand, not a crash: a newer
      // writer must not make an older reader refuse to start.
      return state
  }
}

/** Fold the whole log, in sequence order. */
export function fold(events: readonly LogEvent[]): OrchestratorState {
  const ordered = [...events].sort((left, right) => left.seq - right.seq)
  return ordered.reduce(reduce, EMPTY_STATE)
}

/**
 * Outbox entries replay must deliver now.
 *
 * `now` is a parameter rather than a clock read so backoff is exactly testable
 * and replay is deterministic.
 */
export function dueInputs(state: OrchestratorState, now: number): readonly OutboxEntry[] {
  return Object.values(state.inputs)
    .filter((entry) => entry.state === 'pending' && entry.notBefore <= now)
    .sort((left, right) => (left.createdAt === right.createdAt
      ? left.id.localeCompare(right.id)
      : left.createdAt.localeCompare(right.createdAt)))
}

/** Tickets a human still has to look at. Nothing else belongs on a status line. */
export function attention(state: OrchestratorState): readonly Ticket[] {
  return Object.values(state.tickets)
    .filter((ticket) => ticket.state !== 'settled'
      && (ticket.state !== 'reported' || ticket.report?.status === 'waiting' || ticket.report?.status === 'blocked'))
    .sort((left, right) => left.createdAt.localeCompare(right.createdAt))
}
