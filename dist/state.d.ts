import type { LogEvent } from './log.js';
/** Terminal or in-flight lifecycle of one work ticket. */
export type TicketState = 'created' | 'dispatched' | 'reported' | 'settled';
/** The only statuses a worker may assert about its own work. */
export type ReportStatus = 'done' | 'failed' | 'waiting' | 'blocked';
/** Delivery state of one outbox entry. */
export type InputState = 'pending' | 'acked' | 'failed' | 'cancelled';
/** Structured report a worker files against its own ticket. */
export interface TicketReport {
    readonly status: ReportStatus;
    readonly summary: string;
    /** Required when `status` is `waiting`: the exact question a human must answer. */
    readonly question?: string;
    /** Required when `status` is `blocked`. */
    readonly reason?: string;
    /** Free-form detail the worker chooses to hand back; never read as state. */
    readonly detail?: Record<string, unknown>;
}
export interface Ticket {
    readonly id: string;
    readonly title: string;
    readonly preset?: string;
    readonly cwd?: string;
    readonly createdAt: string;
    /** Worker session this ticket is bound to, once dispatch created or named one. */
    readonly workerSessionId?: string;
    readonly state: TicketState;
    readonly report?: TicketReport;
    readonly reportedBy?: string;
    readonly outcome?: 'done' | 'failed' | 'cancelled';
    readonly note?: string;
}
export interface OutboxEntry {
    readonly id: string;
    readonly ticketId: string;
    readonly sessionId: string;
    /**
     * Client-minted identity reused across every retry of this entry. It is what
     * makes replay reconcile instead of resend blind.
     */
    readonly requestId: string;
    readonly mode: 'queue' | 'steer';
    readonly text: string;
    readonly createdAt: string;
    readonly state: InputState;
    readonly attempts: number;
    /** Epoch milliseconds before which replay must not retry a failed entry. */
    readonly notBefore: number;
    readonly lastError?: string;
}
export interface OrchestratorState {
    readonly tickets: Readonly<Record<string, Ticket>>;
    readonly inputs: Readonly<Record<string, OutboxEntry>>;
}
export declare const EMPTY_STATE: OrchestratorState;
/** A rule violation that is the caller's fault, carrying a stable code. */
export declare class RuleError extends Error {
    readonly code: string;
    constructor(code: string, message: string);
}
/** Exponential backoff with a ceiling, kept pure so tests pin it exactly. */
export declare function backoffMs(attempts: number): number;
/**
 * Validate a report before it can become ticket state.
 *
 * This is the rule the prototype's rejected-design table is really about: a
 * ticket may only be `waiting` because the worker said so in a structured
 * field, never because some sentence in its output looked like a question.
 */
export declare function validateReport(report: TicketReport): TicketReport;
/** Apply one durable event. Pure: same state + same event always yields the same result. */
export declare function reduce(state: OrchestratorState, event: LogEvent): OrchestratorState;
/** Fold the whole log, in sequence order. */
export declare function fold(events: readonly LogEvent[]): OrchestratorState;
/**
 * Outbox entries replay must deliver now.
 *
 * `now` is a parameter rather than a clock read so backoff is exactly testable
 * and replay is deterministic.
 */
export declare function dueInputs(state: OrchestratorState, now: number): readonly OutboxEntry[];
/** Tickets a human still has to look at. Nothing else belongs on a status line. */
export declare function attention(state: OrchestratorState): readonly Ticket[];
