import type { EventLog } from './log.js'
import type { Gateway } from './gateway.js'
import { backoffMs, dueInputs, reduce, type OrchestratorState } from './state.js'

export interface ReplayReport {
  /** Entries replay looked at. */
  readonly considered: number
  /** Entries delivered by this pass. */
  readonly delivered: number
  /** Entries found already present in the session and acknowledged without resending. */
  readonly reconciled: number
  /** Entries that failed and were backed off. */
  readonly deferred: number
  /** Entries still pending afterwards, including the ones backed off. */
  readonly remaining: number
}

/**
 * Deliver everything the outbox still owes, reconciling before resending.
 *
 * The crash this exists for: the process dies after the host accepted a prompt
 * but before the acknowledgement line reached disk. A blind replay would then
 * deliver the same instruction twice. Reconciling first turns "the request id
 * is already in the session" into a local acknowledgement, so the duplicate
 * never leaves the process — and when the probe cannot tell, the retry reuses
 * the same request id, which keeps the duplicate identifiable rather than
 * indistinguishable from a genuinely new instruction.
 */
export async function replayOutbox(options: {
  readonly log: EventLog
  readonly state: OrchestratorState
  readonly gateway: Gateway
  readonly now: number
}): Promise<{ report: ReplayReport; state: OrchestratorState }> {
  const { log, gateway } = options
  let state = options.state

  const due = dueInputs(state, options.now)
  let delivered = 0
  let reconciled = 0
  let deferred = 0

  for (const entry of due) {
    const presence = await gateway.hasRequest(entry.sessionId, entry.requestId)

    if (presence === 'present') {
      state = reduce(state, log.append('input.acked', { inputId: entry.id, reconciled: true }))
      reconciled += 1
      continue
    }

    try {
      await gateway.prompt({
        requestId: entry.requestId,
        sessionId: entry.sessionId,
        mode: entry.mode,
        text: entry.text,
      })
      state = reduce(state, log.append('input.acked', { inputId: entry.id, presence }))
      delivered += 1
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      state = reduce(state, log.append('input.failed', {
        inputId: entry.id,
        error: message,
        notBefore: options.now + backoffMs(entry.attempts + 1),
      }))
      deferred += 1
    }
  }

  return {
    state,
    report: {
      considered: due.length,
      delivered,
      reconciled,
      deferred,
      remaining: Object.values(state.inputs).filter((entry) => entry.state === 'pending').length,
    },
  }
}
