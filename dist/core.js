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
function fakeGateway(options = {}) {
  const created = [];
  const prompts = [];
  let counter = 0;
  const gateway = {
    created,
    prompts,
    presence: options.presence ?? "absent",
    ...options.failWith ? { failWith: options.failWith } : {},
    async createSession(request) {
      created.push(request);
      counter += 1;
      return { sessionId: `worker-${counter}`, ...request.agentPreset ? { agentPreset: request.agentPreset } : {} };
    },
    async prompt(request) {
      if (gateway.failWith) throw gateway.failWith;
      prompts.push(request);
      return { accepted: true };
    },
    async hasRequest() {
      return gateway.presence;
    }
  };
  return gateway;
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
export {
  EMPTY_STATE,
  EventLog,
  RuleError,
  attention,
  backoffMs,
  dueInputs,
  fakeGateway,
  fold,
  liveGateway,
  reduce,
  replayOutbox,
  validateReport
};
