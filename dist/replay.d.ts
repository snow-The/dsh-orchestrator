import type { EventLog } from './log.js';
import type { Gateway } from './gateway.js';
import { type OrchestratorState } from './state.js';
export interface ReplayReport {
    /** Entries replay looked at. */
    readonly considered: number;
    /** Entries delivered by this pass. */
    readonly delivered: number;
    /** Entries found already present in the session and acknowledged without resending. */
    readonly reconciled: number;
    /** Entries that failed and were backed off. */
    readonly deferred: number;
    /** Entries still pending afterwards, including the ones backed off. */
    readonly remaining: number;
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
export declare function replayOutbox(options: {
    readonly log: EventLog;
    readonly state: OrchestratorState;
    readonly gateway: Gateway;
    readonly now: number;
}): Promise<{
    report: ReplayReport;
    state: OrchestratorState;
}>;
