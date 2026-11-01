/**
 * One durable record in the orchestrator event log.
 *
 * `seq` is assigned by the log, never by the caller: it is the total order the
 * fold depends on. `at` is recorded rather than trusted so a replayed log reads
 * the same tomorrow.
 */
export interface LogEvent {
    readonly v: 1;
    readonly seq: number;
    readonly at: string;
    readonly type: string;
    readonly [key: string]: unknown;
}
/** What a load actually recovered, so callers can report repair instead of hiding it. */
export interface LoadReport {
    readonly events: readonly LogEvent[];
    /** Bytes dropped because the final line was an incomplete write. */
    readonly repairedTailBytes: number;
    /** Whole lines skipped because they were not parseable JSON. */
    readonly skippedLines: number;
    /** True when the file was rewritten to restore a trailing newline. */
    readonly rewrote: boolean;
}
/**
 * Append-only JSON-lines event log.
 *
 * Two crash properties are the reason this exists instead of a JSON blob:
 *
 * 1. A process killed mid-write leaves a torn final line. Load drops that line
 *    and rewrites the file so the next append starts on a clean line — the
 *    record we lose is the one that was never acknowledged, not the ones before.
 * 2. Every earlier line survives, because a partial write can only ever be the
 *    tail. That is what makes "state after restart" a fold rather than a guess.
 */
export declare class EventLog {
    readonly file: string;
    private lastSeq;
    constructor(file: string);
    /** Read and repair the log, without mutating durable state beyond repair. */
    load(): LoadReport;
    /**
     * Append one record durably.
     *
     * The write is a single `appendFileSync` of one line followed by an fsync, so
     * a crash either leaves the line absent or leaves it whole; a torn tail is
     * still possible and handled by `load`.
     */
    append(type: string, payload?: Record<string, unknown>): LogEvent;
    /** Highest sequence durably assigned so far. */
    get seq(): number;
    private rewrite;
}
