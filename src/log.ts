import { appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

/**
 * One durable record in the orchestrator event log.
 *
 * `seq` is assigned by the log, never by the caller: it is the total order the
 * fold depends on. `at` is recorded rather than trusted so a replayed log reads
 * the same tomorrow.
 */
export interface LogEvent {
  readonly v: 1
  readonly seq: number
  readonly at: string
  readonly type: string
  readonly [key: string]: unknown
}

/** What a load actually recovered, so callers can report repair instead of hiding it. */
export interface LoadReport {
  readonly events: readonly LogEvent[]
  /** Bytes dropped because the final line was an incomplete write. */
  readonly repairedTailBytes: number
  /** Whole lines skipped because they were not parseable JSON. */
  readonly skippedLines: number
  /** True when the file was rewritten to restore a trailing newline. */
  readonly rewrote: boolean
}

function isEvent(value: unknown): value is LogEvent {
  if (typeof value !== 'object' || value === null) return false
  const record = value as Record<string, unknown>
  return record.v === 1
    && typeof record.seq === 'number'
    && Number.isInteger(record.seq)
    && typeof record.type === 'string'
    && typeof record.at === 'string'
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
export class EventLog {
  private lastSeq = 0

  constructor(readonly file: string) {}

  /** Read and repair the log, without mutating durable state beyond repair. */
  load(): LoadReport {
    if (!existsSync(this.file)) {
      this.lastSeq = 0
      return { events: [], repairedTailBytes: 0, skippedLines: 0, rewrote: false }
    }

    const raw = readFileSync(this.file, 'utf8')
    const endsWithNewline = raw.endsWith('\n')
    const lines = raw.split('\n')
    // A trailing newline yields one empty final element that is not a line.
    if (endsWithNewline) lines.pop()

    let repairedTailBytes = 0
    if (!endsWithNewline && lines.length > 0) {
      repairedTailBytes = Buffer.byteLength(lines.pop() as string, 'utf8')
    }

    const events: LogEvent[] = []
    let skippedLines = 0
    for (const line of lines) {
      if (line.trim() === '') continue
      let parsed: unknown
      try {
        parsed = JSON.parse(line)
      } catch {
        skippedLines += 1
        continue
      }
      if (!isEvent(parsed)) {
        skippedLines += 1
        continue
      }
      events.push(parsed)
    }

    const rewrote = repairedTailBytes > 0 || skippedLines > 0
    if (rewrote) this.rewrite(events)
    this.lastSeq = events.reduce((max, event) => Math.max(max, event.seq), 0)
    return { events, repairedTailBytes, skippedLines, rewrote }
  }

  /**
   * Append one record durably.
   *
   * The write is a single `appendFileSync` of one line followed by an fsync, so
   * a crash either leaves the line absent or leaves it whole; a torn tail is
   * still possible and handled by `load`.
   */
  append(type: string, payload: Record<string, unknown> = {}): LogEvent {
    mkdirSync(dirname(this.file), { recursive: true })
    const event: LogEvent = {
      ...payload,
      v: 1,
      seq: this.lastSeq + 1,
      at: new Date().toISOString(),
      type,
    }
    const bytes = Buffer.from(`${JSON.stringify(event)}\n`, 'utf8')
    const fd = openSync(this.file, 'a')
    try {
      writeSync(fd, bytes)
      fsyncSync(fd)
    } finally {
      closeSync(fd)
    }
    this.lastSeq = event.seq
    return event
  }

  /** Highest sequence durably assigned so far. */
  get seq(): number {
    return this.lastSeq
  }

  private rewrite(events: readonly LogEvent[]): void {
    mkdirSync(dirname(this.file), { recursive: true })
    const body = events.map((event) => `${JSON.stringify(event)}\n`).join('')
    const temp = `${this.file}.repair.tmp`
    writeFileSync(temp, body, 'utf8')
    renameSync(temp, this.file)
    appendFileSync(`${this.file}.repair.log`, `repaired ${new Date().toISOString()} kept=${events.length}\n`, 'utf8')
  }
}
