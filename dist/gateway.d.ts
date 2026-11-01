/** Answer to "did this already land?", with an honest third option. */
export type RequestPresence = 'present' | 'absent' | 'unknown';
/**
 * The only thing the orchestrator needs from the host.
 *
 * It is a seam, not a wrapper: the live implementation talks to
 * `ctx.sessionController`, the fake one is a test double, and the durable
 * logic never imports either.
 */
export interface Gateway {
    createSession(request: {
        cwd?: string;
        agentPreset?: string;
    }): Promise<{
        sessionId: string;
        agentPreset?: string;
    }>;
    prompt(request: {
        readonly requestId: string;
        readonly sessionId: string;
        readonly mode: 'queue' | 'steer';
        readonly text: string;
    }): Promise<{
        accepted: true;
    }>;
    /**
     * Probe whether a prompt carrying `requestId` is already recorded in the
     * session. `unknown` is a first-class answer: a truncated read must not be
     * mistaken for proof of absence.
     */
    hasRequest(sessionId: string, requestId: string): Promise<RequestPresence>;
}
/** The narrow slice of `ctx.sessionController` this plugin depends on. */
export interface SessionControllerLike {
    create(request: {
        cwd?: string;
        agentPreset?: string;
    }): Promise<{
        sessionId: string;
        agentPreset?: string;
    }>;
    /**
     * `signal` is NOT optional here, and that is measured, not assumed: the host
     * implementation calls `signal.throwIfAborted()` before admitting anything, so
     * omitting it fails the very first delivery with
     * `Cannot read properties of undefined (reading 'throwIfAborted')`.
     */
    prompt(request: {
        readonly requestId: string;
        readonly sessionId: string;
        readonly mode: 'queue' | 'steer';
        readonly content: readonly {
            readonly type: 'text';
            readonly text: string;
        }[];
    }, signal: AbortSignal): Promise<{
        accepted: true;
    }>;
    inspect(sessionId: string, signal?: AbortSignal): Promise<unknown>;
}
/**
 * Adapter over the real host service.
 *
 * `hasRequest` scans the inspection payload for the request id rather than
 * depending on the exact shape of a durable event: the id is opaque and
 * unique, so "found anywhere" is sound proof of presence. Absence is weaker —
 * an inspection may return a bounded event prefix — so a miss is reported as
 * `absent` and replay re-sends with the same id, which keeps delivery
 * at-least-once and the duplicate identifiable instead of silent.
 */
export declare function liveGateway(controller: SessionControllerLike): Gateway;
/** Recording fake: every call is observable, and presence is scriptable. */
export declare function fakeGateway(options?: {
    presence?: RequestPresence;
    failWith?: Error;
}): Gateway & {
    readonly created: {
        cwd?: string;
        agentPreset?: string;
    }[];
    readonly prompts: {
        requestId: string;
        sessionId: string;
        mode: 'queue' | 'steer';
        text: string;
    }[];
    presence: RequestPresence;
    failWith?: Error;
};
