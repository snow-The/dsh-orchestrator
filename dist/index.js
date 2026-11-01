// src/index.ts
import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";

// src/log.ts
import { appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
function isEvent(value) {
  if (typeof value !== "object" || value === null) return false;
  const record = value;
  return record.v === 1 && typeof record.seq === "number" && Number.isInteger(record.seq) && typeof record.type === "string" && typeof record.at === "string";
}
var EventLog = class {
  constructor(file) {
    this.file = file;
  }
  lastSeq = 0;
  /** Read and repair the log, without mutating durable state beyond repair. */
  load() {
    if (!existsSync(this.file)) {
      this.lastSeq = 0;
      return { events: [], repairedTailBytes: 0, skippedLines: 0, rewrote: false };
    }
    const raw = readFileSync(this.file, "utf8");
    const endsWithNewline = raw.endsWith("\n");
    const lines = raw.split("\n");
    if (endsWithNewline) lines.pop();
    let repairedTailBytes = 0;
    if (!endsWithNewline && lines.length > 0) {
      repairedTailBytes = Buffer.byteLength(lines.pop(), "utf8");
    }
    const events = [];
    let skippedLines = 0;
    for (const line of lines) {
      if (line.trim() === "") continue;
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        skippedLines += 1;
        continue;
      }
      if (!isEvent(parsed)) {
        skippedLines += 1;
        continue;
      }
      events.push(parsed);
    }
    const rewrote = repairedTailBytes > 0 || skippedLines > 0;
    if (rewrote) this.rewrite(events);
    this.lastSeq = events.reduce((max, event) => Math.max(max, event.seq), 0);
    return { events, repairedTailBytes, skippedLines, rewrote };
  }
  /**
   * Append one record durably.
   *
   * The write is a single `appendFileSync` of one line followed by an fsync, so
   * a crash either leaves the line absent or leaves it whole; a torn tail is
   * still possible and handled by `load`.
   */
  append(type, payload = {}) {
    mkdirSync(dirname(this.file), { recursive: true });
    const event = {
      ...payload,
      v: 1,
      seq: this.lastSeq + 1,
      at: (/* @__PURE__ */ new Date()).toISOString(),
      type
    };
    const bytes = Buffer.from(`${JSON.stringify(event)}
`, "utf8");
    const fd = openSync(this.file, "a");
    try {
      writeSync(fd, bytes);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    this.lastSeq = event.seq;
    return event;
  }
  /** Highest sequence durably assigned so far. */
  get seq() {
    return this.lastSeq;
  }
  rewrite(events) {
    mkdirSync(dirname(this.file), { recursive: true });
    const body = events.map((event) => `${JSON.stringify(event)}
`).join("");
    const temp = `${this.file}.repair.tmp`;
    writeFileSync(temp, body, "utf8");
    renameSync(temp, this.file);
    appendFileSync(`${this.file}.repair.log`, `repaired ${(/* @__PURE__ */ new Date()).toISOString()} kept=${events.length}
`, "utf8");
  }
};

// src/gateway.ts
function liveSignal() {
  return new AbortController().signal;
}
function liveGateway(controller) {
  return {
    async createSession(request) {
      const created = await controller.create({
        ...request.cwd ? { cwd: request.cwd } : {},
        ...request.agentPreset ? { agentPreset: request.agentPreset } : {}
      });
      return {
        sessionId: String(created.sessionId),
        ...created.agentPreset ? { agentPreset: created.agentPreset } : {}
      };
    },
    async prompt(request) {
      const result = await controller.prompt({
        requestId: request.requestId,
        sessionId: request.sessionId,
        mode: request.mode,
        content: [{ type: "text", text: request.text }]
      }, liveSignal());
      if (result.accepted !== true) throw new Error("host did not acknowledge the prompt as accepted");
      return { accepted: true };
    },
    async hasRequest(sessionId, requestId) {
      try {
        const inspection = await controller.inspect(sessionId);
        if (inspection === void 0 || inspection === null) return "unknown";
        return JSON.stringify(inspection).includes(requestId) ? "present" : "absent";
      } catch {
        return "unknown";
      }
    }
  };
}

// src/state.ts
var EMPTY_STATE = { tickets: {}, inputs: {} };
var RuleError = class extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
    this.name = "RuleError";
  }
};
var RETRY_BASE_MS = 1e3;
var RETRY_MAX_MS = 6e4;
function backoffMs(attempts) {
  const exponent = Math.max(0, Math.min(16, attempts - 1));
  return Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** exponent);
}
function requireString(payload, key, event) {
  const value = payload[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new RuleError("INVALID_EVENT", `${event} requires a non-empty "${key}"`);
  }
  return value;
}
function validateReport(report) {
  if (!["done", "failed", "waiting", "blocked"].includes(report.status)) {
    throw new RuleError("INVALID_REPORT", `unknown report status: ${String(report.status)}`);
  }
  if (typeof report.summary !== "string" || report.summary.trim() === "") {
    throw new RuleError("INVALID_REPORT", "report requires a non-empty summary");
  }
  if (report.status === "waiting" && (typeof report.question !== "string" || report.question.trim() === "")) {
    throw new RuleError("INVALID_REPORT", 'status "waiting" requires a non-empty question');
  }
  if (report.status === "blocked" && (typeof report.reason !== "string" || report.reason.trim() === "")) {
    throw new RuleError("INVALID_REPORT", 'status "blocked" requires a non-empty reason');
  }
  return report;
}
function reduce(state, event) {
  const payload = event;
  switch (event.type) {
    case "ticket.created": {
      const id = requireString(payload, "ticketId", event.type);
      if (state.tickets[id]) throw new RuleError("TICKET_EXISTS", `ticket ${id} already exists`);
      const ticket = {
        id,
        title: requireString(payload, "title", event.type),
        state: "created",
        createdAt: event.at,
        ...typeof payload.preset === "string" ? { preset: payload.preset } : {},
        ...typeof payload.cwd === "string" ? { cwd: payload.cwd } : {}
      };
      return { ...state, tickets: { ...state.tickets, [id]: ticket } };
    }
    case "ticket.bound": {
      const id = requireString(payload, "ticketId", event.type);
      const ticket = state.tickets[id];
      if (!ticket) throw new RuleError("UNKNOWN_TICKET", `unknown ticket ${id}`);
      const sessionId = requireString(payload, "workerSessionId", event.type);
      return {
        ...state,
        tickets: { ...state.tickets, [id]: { ...ticket, workerSessionId: sessionId } }
      };
    }
    case "input.enqueued": {
      const id = requireString(payload, "inputId", event.type);
      if (state.inputs[id]) throw new RuleError("INPUT_EXISTS", `input ${id} already exists`);
      const ticketId = requireString(payload, "ticketId", event.type);
      const ticket = state.tickets[ticketId];
      if (!ticket) throw new RuleError("UNKNOWN_TICKET", `unknown ticket ${ticketId}`);
      const entry = {
        id,
        ticketId,
        sessionId: requireString(payload, "sessionId", event.type),
        requestId: requireString(payload, "requestId", event.type),
        mode: payload.mode === "steer" ? "steer" : "queue",
        text: requireString(payload, "text", event.type),
        createdAt: event.at,
        state: "pending",
        attempts: 0,
        notBefore: 0
      };
      return { ...state, inputs: { ...state.inputs, [id]: entry } };
    }
    case "input.acked": {
      const id = requireString(payload, "inputId", event.type);
      const entry = state.inputs[id];
      if (!entry) throw new RuleError("UNKNOWN_INPUT", `unknown input ${id}`);
      if (entry.state === "acked") return state;
      const inputs = { ...state.inputs, [id]: { ...entry, state: "acked" } };
      const ticket = state.tickets[entry.ticketId];
      const dispatched = ticket && ticket.state === "created" ? { ...ticket, state: "dispatched" } : ticket;
      return {
        inputs,
        tickets: ticket ? { ...state.tickets, [entry.ticketId]: dispatched } : state.tickets
      };
    }
    case "input.failed": {
      const id = requireString(payload, "inputId", event.type);
      const entry = state.inputs[id];
      if (!entry) throw new RuleError("UNKNOWN_INPUT", `unknown input ${id}`);
      if (entry.state === "acked" || entry.state === "cancelled") return state;
      const attempts = entry.attempts + 1;
      const error = typeof payload.error === "string" ? payload.error : "unknown error";
      const notBefore = typeof payload.notBefore === "number" && Number.isFinite(payload.notBefore) ? payload.notBefore : Date.parse(event.at) + backoffMs(attempts);
      const inputs = {
        ...state.inputs,
        [id]: {
          ...entry,
          state: "pending",
          attempts,
          notBefore,
          lastError: error
        }
      };
      return { ...state, inputs };
    }
    case "ticket.reported": {
      const id = requireString(payload, "ticketId", event.type);
      const ticket = state.tickets[id];
      if (!ticket) throw new RuleError("UNKNOWN_TICKET", `unknown ticket ${id}`);
      if (ticket.state === "settled") throw new RuleError("TICKET_SETTLED", `ticket ${id} is already settled`);
      if (ticket.report) throw new RuleError("ALREADY_REPORTED", `ticket ${id} already has a report`);
      const by = requireString(payload, "bySessionId", event.type);
      if (!ticket.workerSessionId) {
        throw new RuleError("TICKET_UNBOUND", `ticket ${id} has no worker session to report from`);
      }
      if (by !== ticket.workerSessionId) {
        throw new RuleError("FOREIGN_SESSION", `ticket ${id} is bound to ${ticket.workerSessionId}, not ${by}`);
      }
      const report = validateReport(payload.report);
      return {
        ...state,
        tickets: { ...state.tickets, [id]: { ...ticket, state: "reported", report, reportedBy: by } }
      };
    }
    case "ticket.settled": {
      const id = requireString(payload, "ticketId", event.type);
      const ticket = state.tickets[id];
      if (!ticket) throw new RuleError("UNKNOWN_TICKET", `unknown ticket ${id}`);
      if (ticket.state === "settled") throw new RuleError("TICKET_SETTLED", `ticket ${id} is already settled`);
      const outcome = payload.outcome;
      if (outcome !== "done" && outcome !== "failed" && outcome !== "cancelled") {
        throw new RuleError("INVALID_EVENT", `ticket.settled requires outcome done|failed|cancelled, got ${String(outcome)}`);
      }
      const inputs = { ...state.inputs };
      for (const [inputId, entry] of Object.entries(state.inputs)) {
        if (entry.ticketId === id && entry.state === "pending") {
          inputs[inputId] = { ...entry, state: "cancelled" };
        }
      }
      return {
        inputs,
        tickets: {
          ...state.tickets,
          [id]: {
            ...ticket,
            state: "settled",
            outcome,
            ...typeof payload.note === "string" ? { note: payload.note } : {}
          }
        }
      };
    }
    default:
      return state;
  }
}
function fold(events) {
  const ordered = [...events].sort((left, right) => left.seq - right.seq);
  return ordered.reduce(reduce, EMPTY_STATE);
}
function dueInputs(state, now) {
  return Object.values(state.inputs).filter((entry) => entry.state === "pending" && entry.notBefore <= now).sort((left, right) => left.createdAt === right.createdAt ? left.id.localeCompare(right.id) : left.createdAt.localeCompare(right.createdAt));
}
function attention(state) {
  return Object.values(state.tickets).filter((ticket) => ticket.state !== "settled" && (ticket.state !== "reported" || ticket.report?.status === "waiting" || ticket.report?.status === "blocked")).sort((left, right) => left.createdAt.localeCompare(right.createdAt));
}

// src/replay.ts
async function replayOutbox(options) {
  const { log, gateway } = options;
  let state = options.state;
  const due = dueInputs(state, options.now);
  let delivered = 0;
  let reconciled = 0;
  let deferred = 0;
  for (const entry of due) {
    const presence = await gateway.hasRequest(entry.sessionId, entry.requestId);
    if (presence === "present") {
      state = reduce(state, log.append("input.acked", { inputId: entry.id, reconciled: true }));
      reconciled += 1;
      continue;
    }
    try {
      await gateway.prompt({
        requestId: entry.requestId,
        sessionId: entry.sessionId,
        mode: entry.mode,
        text: entry.text
      });
      state = reduce(state, log.append("input.acked", { inputId: entry.id, presence }));
      delivered += 1;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      state = reduce(state, log.append("input.failed", {
        inputId: entry.id,
        error: message,
        notBefore: options.now + backoffMs(entry.attempts + 1)
      }));
      deferred += 1;
    }
  }
  return {
    state,
    report: {
      considered: due.length,
      delivered,
      reconciled,
      deferred,
      remaining: Object.values(state.inputs).filter((entry) => entry.state === "pending").length
    }
  };
}

// src/index.ts
var name = "orchestrator";
var inject = ["tools"];
var textOutput = () => ({
  schema: { type: "object", additionalProperties: true },
  render: (_args, value) => [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }]
});
function sessionIdOf(exec) {
  const agent = exec?.agent;
  const id = agent?.id;
  return typeof id === "string" && id !== "" ? id : void 0;
}
function fail(code, message, extra = {}) {
  return { code, message, ok: false, ...extra };
}
function asFailure(error) {
  if (error instanceof RuleError) return fail(error.code, error.message);
  return fail("INTERNAL", error instanceof Error ? error.message : String(error));
}
function requiredArg(args, key) {
  const value = args[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new RuleError("INVALID_ARGS", `"${key}" is required and must be a non-empty string (received ${value === void 0 ? "undefined" : typeof value})`);
  }
  return value;
}
function apply(ctx, options = {}) {
  const dshHome = options.dshHome ?? process.env.DSH_HOME ?? join(homedir(), ".dsh");
  const log = new EventLog(join(dshHome, "orchestrator", "events.jsonl"));
  let state = EMPTY_STATE;
  let loaded = false;
  let replayChain = Promise.resolve();
  function ready() {
    if (!loaded) {
      state = fold(log.load().events);
      loaded = true;
    }
    return state;
  }
  function commit(type, payload) {
    const current = ready();
    const event = log.append(type, payload);
    state = reduce(current, event);
    return state;
  }
  function controller() {
    const service = ctx.get?.("sessionController");
    return service ?? void 0;
  }
  function gateway() {
    const service = controller();
    return service ? liveGateway(service) : void 0;
  }
  function replay() {
    const run = replayChain.then(async () => {
      const gw = gateway();
      if (!gw) return fail("NO_SESSION_CONTROLLER", "ctx.sessionController is unavailable, so the outbox cannot be drained");
      const current = ready();
      const { report, state: next } = await replayOutbox({ log, state: current, gateway: gw, now: Date.now() });
      state = next;
      return { ok: true, ...report };
    });
    replayChain = run.catch(() => void 0);
    return run;
  }
  function summary(target) {
    const tickets = Object.values(target.tickets);
    const inputs = Object.values(target.inputs);
    const byState = {};
    for (const ticket of tickets) byState[ticket.state] = (byState[ticket.state] ?? 0) + 1;
    return {
      tickets: tickets.length,
      tickets_by_state: byState,
      inputs: inputs.length,
      inputs_pending: inputs.filter((entry) => entry.state === "pending").length,
      inputs_failed: inputs.filter((entry) => entry.state === "pending" && entry.attempts > 0).length
    };
  }
  try {
    ready();
    log.append("plugin.mounted", { dshHome, pid: process.pid, version: "0.1.0" });
  } catch {
  }
  const mountReplay = () => {
    void replay().catch(() => void 0);
  };
  if (ctx.inject) {
    ctx.inject(["sessionController"], () => {
      mountReplay();
    });
  } else {
    mountReplay();
  }
  ctx.tools.register({
    name: "orchestrator_open",
    description: "Open a work ticket and dispatch it to a fresh worker session. The ticket and its first instruction are written to a durable log before anything is sent, so a crash between writing and sending loses neither. Returns the ticket and what the replay pass actually delivered.",
    parameters: {
      properties: {
        cwd: { description: "Workspace the worker session owns. Defaults to the host default.", type: "string" },
        dispatch: { description: "Create the worker session and send the prompt now. Default true.", type: "boolean" },
        preset: { description: "Agent preset the worker session runs, e.g. a restricted worker preset.", type: "string" },
        prompt: { description: "The instruction the worker receives. Recorded verbatim except that {{ticket}} is replaced by this ticket id, so the opening instruction can tell the worker which ticket to report against.", type: "string" },
        title: { description: "Short human-readable title for the ticket.", type: "string" }
      },
      required: ["title", "prompt"],
      type: "object"
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        const title = requiredArg(args, "title");
        const promptText = requiredArg(args, "prompt");
        const current = ready();
        const ticketId = `t_${randomUUID().slice(0, 8)}`;
        commit("ticket.created", {
          ticketId,
          title,
          ...typeof args.preset === "string" ? { preset: args.preset } : {},
          ...typeof args.cwd === "string" ? { cwd: args.cwd } : {}
        });
        if (args.dispatch === false) {
          return { ok: true, delivered: null, ticket: state.tickets[ticketId], ticket_before: current.tickets[ticketId] ?? null };
        }
        const gw = gateway();
        if (!gw) return fail("NO_SESSION_CONTROLLER", "ctx.sessionController is unavailable, so no worker session can be created");
        const created = await gw.createSession({
          ...typeof args.cwd === "string" ? { cwd: args.cwd } : {},
          ...typeof args.preset === "string" ? { agentPreset: args.preset } : {}
        });
        commit("ticket.bound", { ticketId, workerSessionId: created.sessionId });
        const inputId = `i_${randomUUID().slice(0, 8)}`;
        const text = promptText.replaceAll("{{ticket}}", ticketId);
        commit("input.enqueued", {
          inputId,
          ticketId,
          sessionId: created.sessionId,
          requestId: `req_${randomUUID()}`,
          mode: "queue",
          text
        });
        const delivered = await replay();
        return { ok: true, delivered, ticket: state.tickets[ticketId] };
      } catch (error) {
        return asFailure(error);
      }
    }
  });
  ctx.tools.register({
    name: "orchestrator_send",
    description: `Queue one more instruction for an existing ticket's worker. Use mode "steer" to interrupt the current turn and "queue" (default) to append. Delivery goes through the same durable outbox as the first instruction.`,
    parameters: {
      properties: {
        mode: { description: '"queue" appends after the current turn; "steer" interrupts it. Default queue.', type: "string" },
        prompt: { description: "Instruction text, recorded verbatim.", type: "string" },
        ticket: { description: "Ticket id, e.g. t_1a2b3c4d.", type: "string" }
      },
      required: ["ticket", "prompt"],
      type: "object"
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready();
        const ticketId = requiredArg(args, "ticket");
        const ticket = state.tickets[ticketId];
        if (!ticket) return fail("UNKNOWN_TICKET", `no ticket ${ticketId}`);
        if (ticket.state === "settled") return fail("TICKET_SETTLED", `ticket ${ticketId} is already settled`);
        if (!ticket.workerSessionId) return fail("TICKET_UNBOUND", `ticket ${ticketId} has no worker session`);
        const inputId = `i_${randomUUID().slice(0, 8)}`;
        commit("input.enqueued", {
          inputId,
          ticketId,
          sessionId: ticket.workerSessionId,
          requestId: `req_${randomUUID()}`,
          mode: args.mode === "steer" ? "steer" : "queue",
          text: requiredArg(args, "prompt")
        });
        const delivered = await replay();
        return { ok: true, delivered, input: state.inputs[inputId], ticket: state.tickets[ticketId] };
      } catch (error) {
        return asFailure(error);
      }
    }
  });
  ctx.tools.register({
    name: "orchestrator_report",
    description: 'File the structured report for a ticket you are working. This is the ONLY way a ticket becomes reported: prose in your final message is not read as state. Use status "waiting" with a concrete question when a human decision is required, and "done" / "failed" / "blocked" otherwise.',
    parameters: {
      properties: {
        detail: { description: "Free-form structured detail for the human. Never interpreted as ticket state.", type: "object" },
        question: { description: 'Required when status is "waiting": the exact question a human must answer.', type: "string" },
        reason: { description: 'Required when status is "blocked".', type: "string" },
        status: { description: "One of done | failed | waiting | blocked.", type: "string" },
        summary: { description: "One or two sentences of what happened.", type: "string" },
        ticket: { description: "Ticket id you were given.", type: "string" }
      },
      required: ["ticket", "status", "summary"],
      type: "object"
    },
    output: textOutput(),
    execute: async (args, exec) => {
      try {
        ready();
        const by = sessionIdOf(exec);
        if (!by) return fail("NO_SESSION_ID", "this tool call carries no session identity (exec.agent.id is absent)");
        const ticketId = requiredArg(args, "ticket");
        const report = {
          status: requiredArg(args, "status"),
          summary: requiredArg(args, "summary"),
          ...typeof args.question === "string" ? { question: args.question } : {},
          ...typeof args.reason === "string" ? { reason: args.reason } : {},
          ...typeof args.detail === "object" && args.detail !== null ? { detail: args.detail } : {}
        };
        commit("ticket.reported", { bySessionId: by, report, ticketId });
        const ticket = state.tickets[ticketId];
        return { ok: true, needs_human: ticket.report?.status === "waiting" || ticket.report?.status === "blocked", ticket };
      } catch (error) {
        return asFailure(error);
      }
    }
  });
  ctx.tools.register({
    name: "orchestrator_settle",
    description: "Close a ticket for good: mark it done, failed, or cancelled. Settled tickets leave the attention list and are never dispatched again.",
    parameters: {
      properties: {
        note: { description: "Optional closing note.", type: "string" },
        outcome: { description: "done | failed | cancelled.", type: "string" },
        ticket: { description: "Ticket id.", type: "string" }
      },
      required: ["ticket", "outcome"],
      type: "object"
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready();
        const ticketId = requiredArg(args, "ticket");
        commit("ticket.settled", {
          ticketId,
          outcome: requiredArg(args, "outcome"),
          ...typeof args.note === "string" ? { note: args.note } : {}
        });
        return { ok: true, ticket: state.tickets[ticketId] };
      } catch (error) {
        return asFailure(error);
      }
    }
  });
  ctx.tools.register({
    name: "orchestrator_status",
    description: "Show the tickets that actually need a human and what the outbox still owes. Pass replay:true to drain the outbox first. Pass all:true to include settled tickets.",
    parameters: {
      properties: {
        all: { description: "Include settled tickets. Default false.", type: "boolean" },
        replay: { description: "Drain the outbox before reporting. Default false.", type: "boolean" }
      },
      type: "object"
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        ready();
        const delivered = args.replay === true ? await replay() : null;
        const current = state;
        const tickets = args.all === true ? Object.values(current.tickets) : attention(current);
        const pending = Object.values(current.inputs).filter((entry) => entry.state === "pending");
        const due = dueInputs(current, Date.now());
        return {
          ok: true,
          delivered,
          due_now: due.map((entry) => ({ attempts: entry.attempts, id: entry.id, ticket: entry.ticketId })),
          outbox_pending: pending.map((entry) => ({
            attempts: entry.attempts,
            id: entry.id,
            last_error: entry.lastError ?? null,
            not_before: entry.notBefore > 0 ? new Date(entry.notBefore).toISOString() : null,
            ticket: entry.ticketId
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
            worker_session_id: ticket.workerSessionId ?? null
          }))
        };
      } catch (error) {
        return asFailure(error);
      }
    },
    isConcurrencySafe: () => true
  });
  ctx.tools.register({
    name: "orchestrator_reachability",
    description: 'Report how many persisted sessions could actually be woken: the host marks each with agentAvailable, so "reachable" is measured, not guessed. Read-only \u2014 it never resumes a session, because resuming would make this plugin the owner of a session it does not own. Use before dispatching work to a cold worker.',
    parameters: {
      properties: {
        sample: { description: "How many unreachable session ids to name. Default 5.", type: "number" }
      },
      type: "object"
    },
    output: textOutput(),
    execute: async (args) => {
      try {
        const controller2 = ctx.get?.("sessionController");
        const agents = ctx.get?.("agents");
        if (!controller2?.list) {
          return fail("NO_SESSION_CONTROLLER", "ctx.sessionController.list is unavailable, so the session set cannot be read");
        }
        const response = await controller2.list({}, new AbortController().signal);
        const items = response?.items ?? [];
        const ids = items.map((item) => String(item.sessionId));
        const available = items.filter((item) => item.agentAvailable === true);
        const unreachable = items.filter((item) => item.agentAvailable !== true);
        const sampleSize = typeof args.sample === "number" && Number.isFinite(args.sample) ? Math.max(0, Math.min(50, args.sample)) : 5;
        const registrySays = typeof agents?.get === "function" ? ids.filter((id) => agents.get?.(id) !== void 0).length : null;
        const registryList = typeof agents?.list === "function" ? agents.list().length : null;
        return {
          ok: true,
          agents_get_agrees: registrySays === null ? null : registrySays === available.length,
          live_agent_count: registryList,
          reachable: available.length,
          reachable_by_registry: registrySays,
          sessions: ids.length,
          unreachable: unreachable.length,
          unreachable_sample: unreachable.slice(0, sampleSize).map((item) => ({
            blank: item.blank === true,
            id: String(item.sessionId),
            origin: typeof item.origin === "string" ? item.origin : null,
            running: item.running === true
          })),
          verdict: ids.length === 0 ? "no sessions reported" : available.length === ids.length ? "every session owns a live agent" : "wake reaches only the sessions that already own a live agent (C10)"
        };
      } catch (error) {
        return asFailure(error);
      }
    },
    isConcurrencySafe: () => true
  });
}
export {
  apply,
  inject,
  name
};
