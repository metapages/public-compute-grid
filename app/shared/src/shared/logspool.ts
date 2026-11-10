/**
 * An on-disk copy of the logs a queue is buffering for its live jobs (#360).
 *
 * BaseDockerJobQueue holds the build and run logs streaming in from a worker in
 * memory until the job finishes, then stores them in the db. A server that
 * restarts mid-job used to lose every line streamed so far. Lines given to the
 * spool are appended to a file per job and kind, and a restarted server loads
 * them back as its buffers.
 *
 * Appends are batched: lines are written at most `flushInterval` after they
 * arrive. That bounds what an abrupt kill (SIGKILL, power loss) can lose to the
 * last `flushInterval` of output. A torn last line is skipped on load.
 *
 * Files are JSON lines (one ConsoleLogLine per line), under
 * `<directory>/<jobId>.<kind>.jsonl`. The directory should be one the next
 * server process sees too: the local-mode worker's data directory, or a volume
 * shared by the api instances. Not in the db (Deno KV): values are capped at
 * 64 KiB, and a job's logs are much bigger than that.
 *
 * Every error is logged and swallowed: losing the spool must never break the
 * queue, it only means a restart loses logs like it did before.
 *
 * node:fs, not std/fs: this module is in the Cloudflare Worker's module graph
 * (through jobqueue.ts), where std/fs cannot even be imported. It is never
 * instantiated there.
 */
import { appendFile, mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { ConsoleLogLine } from "@shared/types.ts";

export type LogKind = "build" | "run";

const KINDS: LogKind[] = ["build", "run"];
const SUFFIX = ".jsonl";

export type SpooledLogs = {
  jobId: string;
  kind: LogKind;
  lines: ConsoleLogLine[];
  /** Last write to the file, ms since epoch */
  modified: number;
};

/** File names are built from job ids: keep them to a safe alphabet. */
const safeName = (jobId: string) => jobId.replace(/[^a-zA-Z0-9_-]/g, "_");

export class LogSpool {
  readonly directory: string;
  private readonly flushInterval: number;
  private readonly maxLines: number;
  /** Lines not written yet, by file */
  private pending = new Map<string, ConsoleLogLine[]>();
  /** Lines in each file this process has written to, to know when to compact */
  private counts = new Map<string, number>();
  /** Per file, its writes chained so they happen one at a time and in order */
  private writes = new Map<string, Promise<void>>();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private directoryReady: Promise<void> | undefined;

  constructor(opts: { directory: string; flushInterval?: number; maxLines: number }) {
    this.directory = opts.directory;
    this.flushInterval = opts.flushInterval ?? 1000;
    this.maxLines = opts.maxLines;
  }

  private file(jobId: string, kind: LogKind) {
    return join(this.directory, `${safeName(jobId)}.${kind}${SUFFIX}`);
  }

  /** Queue lines to be written within flushInterval. */
  append(jobId: string, kind: LogKind, lines: ConsoleLogLine[]): void {
    if (!lines.length) {
      return;
    }
    const file = this.file(jobId, kind);
    const pending = this.pending.get(file);
    if (pending) {
      pending.push(...lines);
    } else {
      this.pending.set(file, [...lines]);
    }
    if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.flush();
      }, this.flushInterval);
      // A pending flush must not keep a process alive that is otherwise done
      (globalThis as { Deno?: { unrefTimer(id: number): void } }).Deno?.unrefTimer(this.timer as unknown as number);
    }
  }

  /** Write every pending line now. Settles once they (and earlier writes) are on disk. */
  flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    const pending = this.pending;
    this.pending = new Map();
    return Promise.all(
      [...pending].map(([file, lines]) => this.chain(file, () => this.write(file, lines))),
    ).then(() => {});
  }

  /** Forget a job's spooled logs (every kind if none given): pending lines and the files. */
  clear(jobId: string, kind?: LogKind): Promise<void> {
    return Promise.all((kind ? [kind] : KINDS).map((k) => {
      const file = this.file(jobId, k);
      this.pending.delete(file);
      this.counts.delete(file);
      return this.chain(file, () => rm(file, { force: true }));
    })).then(() => {});
  }

  /** Everything on disk, for a server starting up. Each file capped to its last maxLines. */
  async load(): Promise<SpooledLogs[]> {
    let names: string[];
    try {
      names = await readdir(this.directory);
    } catch (_) {
      // Nothing spooled yet
      return [];
    }
    const results: SpooledLogs[] = [];
    for (const name of names) {
      const match = name.match(/^(.+)\.(build|run)\.jsonl$/);
      if (!match) {
        continue;
      }
      const [, jobId, kind] = match;
      const file = join(this.directory, name);
      try {
        await this.writes.get(file);
        const [text, info] = await Promise.all([readFile(file, "utf8"), stat(file)]);
        const lines = parseLines(text);
        this.counts.set(file, lines.length);
        results.push({
          jobId,
          kind: kind as LogKind,
          lines: lines.length > this.maxLines ? lines.slice(-this.maxLines) : lines,
          modified: info.mtimeMs,
        });
      } catch (err) {
        console.error(`Failed to load spooled logs ${file}:`, err);
      }
    }
    return results;
  }

  private chain(file: string, write: () => Promise<void>): Promise<void> {
    const next = (this.writes.get(file) ?? Promise.resolve()).then(write).catch((err) => {
      console.error(`Failed to write spooled logs ${file}:`, err);
    });
    this.writes.set(file, next);
    // Drop the entry once settled, unless another write was chained since
    next.then(() => {
      if (this.writes.get(file) === next) {
        this.writes.delete(file);
      }
    });
    return next;
  }

  private async write(file: string, lines: ConsoleLogLine[]): Promise<void> {
    this.directoryReady ??= mkdir(this.directory, { recursive: true }).then(() => {});
    try {
      await this.directoryReady;
    } catch (err) {
      // Try again next time: the directory may have been deleted under us
      this.directoryReady = undefined;
      throw err;
    }
    await appendFile(file, toText(lines));
    const count = (this.counts.get(file) ?? 0) + lines.length;
    this.counts.set(file, count);
    // A runaway container can emit unbounded output. The buffers keep the
    // tail, so the file only needs to as well: rewrite it to the last maxLines
    // once it is twice that, so it costs a rewrite per maxLines lines.
    if (count > 2 * this.maxLines) {
      const tail = parseLines(await readFile(file, "utf8")).slice(-this.maxLines);
      const tmp = `${file}.tmp`;
      await writeFile(tmp, toText(tail));
      await rename(tmp, file);
      this.counts.set(file, tail.length);
    }
  }
}

/**
 * Log lines to JSON lines. Each write starts with its own newline, so a line
 * torn by a kill is never glued onto the first line of the next write.
 */
const toText = (lines: ConsoleLogLine[]) => lines.map((line) => "\n" + JSON.stringify(line)).join("");

/** JSON lines to log lines, skipping any that do not parse (a write torn by a kill). */
const parseLines = (text: string): ConsoleLogLine[] => {
  const lines: ConsoleLogLine[] = [];
  for (const row of text.split("\n")) {
    if (!row) {
      continue;
    }
    try {
      const line = JSON.parse(row);
      if (Array.isArray(line) && typeof line[0] === "string") {
        lines.push(line as ConsoleLogLine);
      }
    } catch (_) {
      // torn write
    }
  }
  return lines;
};
