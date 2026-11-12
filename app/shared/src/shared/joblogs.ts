/**
 * Job build and run logs, written to storage as they stream in (#360).
 *
 * A worker streams a job's logs to the server holding its socket. They used to
 * sit in that server's memory until the job finished, so a restart mid-job lost
 * them. Now the lines are written to storage at most `flushInterval` after
 * they arrive, and memory only holds the ones not written yet (and the pipe to
 * the websockets, see BaseDockerJobQueue.sendLogsToLocalClients).
 *
 * Storage is the same place as the rest of a job's files (LogStorage): the
 * bucket (R2 in production, MinIO in the docker stacks) when there is one, the
 * data directory on disk otherwise (local mode). Objects cannot be appended
 * to, so each flush writes one more chunk:
 *
 *   j/<jobId>/logs/<kind>/<time>-<instance>-<seq>.json   chunks, while live
 *   j/<jobId>/logs/<kind>.json                           the whole log, once finished
 *
 * When the job finishes the chunks are merged into the single file and deleted.
 * A reader takes the file and then the chunks, in name order (time first), as
 * one log; the API streams that as a single JSON array (/j/<id>/logs.<kind>.json).
 *
 * What can be lost: the lines of the last `flushInterval` before the process is
 * killed outright (a clean SIGTERM/SIGINT flushes first, see the worker CLI).
 * And a reader on another instance in the moment a finished job's chunks are
 * being merged sees the merged file and the chunks not yet deleted, so a line
 * twice. On the instance doing the merge, reads wait for it.
 *
 * Every storage error is logged and swallowed: lines that fail to write are
 * kept and retried on the next flush. Losing logs must never break the queue.
 */
import { LRUMapWithDelete } from "mnemonist";
import { mkdir, readdir, readFile, rename, rm, rmdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import type { DB } from "@shared/db.ts";
import { getEnv } from "@shared/env.ts";
import { getBlobStore } from "@shared/s3.ts";
import type { ConsoleLogLine } from "@shared/types.ts";

export type LogKind = "build" | "run";

const KINDS: LogKind[] = ["build", "run"];

/**
 * A job's log lines are capped to their last this many when the chunks are
 * merged: a runaway container can emit unbounded output, and the tail is where
 * the error is.
 */
export const MAX_LOG_LINES = 10000;

/** The whole log of a finished job */
export const formatJobLogsKey = (jobId: string, kind: LogKind): string => `j/${jobId}/logs/${kind}.json`;

/** Where a live job's chunks are */
export const formatJobLogChunksPrefix = (jobId: string, kind: LogKind): string => `j/${jobId}/logs/${kind}/`;

/**
 * Where log objects are kept: a bucket or a directory. Keys are paths
 * ("j/<id>/logs/run.json"); `list` takes a prefix ending in "/" and returns the
 * keys directly under it, sorted.
 */
export interface LogStorage {
  get(key: string): Promise<string | undefined>;
  put(key: string, body: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  delete(key: string): Promise<void>;
}

/** Log objects as files under a directory (local mode: the data directory). */
export class DiskLogStorage implements LogStorage {
  constructor(private readonly directory: string) {}

  async get(key: string): Promise<string | undefined> {
    try {
      return await readFile(join(this.directory, key), "utf8");
    } catch (err) {
      if ((err as { code?: string }).code === "ENOENT") {
        return undefined;
      }
      throw err;
    }
  }

  async put(key: string, body: string): Promise<void> {
    const file = join(this.directory, key);
    await mkdir(dirname(file), { recursive: true });
    // Written whole and renamed into place, so a reader never sees half of it
    const tmp = `${file}.${crypto.randomUUID()}.tmp`;
    await writeFile(tmp, body);
    await rename(tmp, file);
  }

  async list(prefix: string): Promise<string[]> {
    try {
      const entries = await readdir(join(this.directory, prefix), { withFileTypes: true });
      return entries
        .filter((entry) => entry.isFile() && !entry.name.endsWith(".tmp"))
        .map((entry) => prefix + entry.name)
        .sort();
    } catch (err) {
      if ((err as { code?: string }).code === "ENOENT") {
        return [];
      }
      throw err;
    }
  }

  async delete(key: string): Promise<void> {
    const file = join(this.directory, key);
    await rm(file, { force: true });
    // Like a bucket, which has no directories: drop the chunks' once empty
    await rmdir(dirname(file)).catch(() => {});
  }
}

/** Log objects in the bucket (R2, MinIO, S3). */
export class BucketLogStorage implements LogStorage {
  get(key: string): Promise<string | undefined> {
    return getBlobStore().getText(key);
  }
  put(key: string, body: string): Promise<void> {
    return getBlobStore().putText(key, body);
  }
  list(prefix: string): Promise<string[]> {
    return getBlobStore().list(prefix);
  }
  delete(key: string): Promise<void> {
    return getBlobStore().delete(key);
  }
}

/**
 * The bucket when the server has one, else the data directory: the same
 * choice DB.initialize makes for the rest of a job's files.
 */
export const getLogStorage = (dataDirectory: string): LogStorage =>
  getEnv("AWS_SECRET_ACCESS_KEY") ? new BucketLogStorage() : new DiskLogStorage(dataDirectory);

const parseLines = (body: string | undefined): ConsoleLogLine[] => {
  if (!body) {
    return [];
  }
  try {
    const lines = JSON.parse(body);
    return Array.isArray(lines) ? lines : [];
  } catch (err) {
    console.error(`Unreadable log object:`, err);
    return [];
  }
};

/**
 * Job ids are hashes. One that is not (a URL parameter can be anything) would
 * name objects outside the job's directory, so it has no logs.
 */
const isSafeJobId = (jobId: string) => /^[a-zA-Z0-9_-]+$/.test(jobId);

/** The keys of a job's stored log, in order: the merged file (if any), then the chunks. */
const storedKeys = async (storage: LogStorage, jobId: string, kind: LogKind): Promise<string[]> => {
  if (!isSafeJobId(jobId)) {
    return [];
  }
  const [final, chunks] = await Promise.all([
    storage.get(formatJobLogsKey(jobId, kind)).then((body) => body !== undefined),
    storage.list(formatJobLogChunksPrefix(jobId, kind)),
  ]);
  return final ? [formatJobLogsKey(jobId, kind), ...chunks] : chunks;
};

/** A job's stored log as one list of lines: the merged file, then any chunks. */
export const readStoredLogs = async (
  storage: LogStorage,
  jobId: string,
  kind: LogKind,
): Promise<ConsoleLogLine[]> => {
  const bodies = await Promise.all((await storedKeys(storage, jobId, kind)).map((key) => storage.get(key)));
  return bodies.flatMap(parseLines);
};

/**
 * A job's stored log as a single JSON array, streamed object by object without
 * parsing them, or undefined if nothing is stored for it.
 */
export const streamStoredLogs = async (
  storage: LogStorage,
  jobId: string,
  kind: LogKind,
): Promise<ReadableStream<Uint8Array> | undefined> => {
  const keys = await storedKeys(storage, jobId, kind);
  if (!keys.length) {
    return undefined;
  }
  const encoder = new TextEncoder();
  let index = 0;
  let wroteLine = false;
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode("["));
    },
    // Enqueues something every call: a pull that enqueues nothing is not called again
    async pull(controller) {
      while (index < keys.length) {
        const body = (await storage.get(keys[index++]))?.trim() ?? "";
        // Each object is a JSON array: its inside is a list of lines
        const inner = body.startsWith("[") && body.endsWith("]") ? body.slice(1, -1).trim() : "";
        if (inner) {
          controller.enqueue(encoder.encode((wroteLine ? "," : "") + inner));
          wroteLine = true;
          return;
        }
      }
      controller.enqueue(encoder.encode("]"));
      controller.close();
    },
  });
};

/**
 * A job's log as one JSON array of lines, the response of
 * GET /j/<jobId>/logs.<kind>.json: streamed from storage, merged file and
 * chunks alike, so a live job's log so far reads the same as a finished one.
 * Jobs finished before logs were stored like this have them in the db. A
 * known job with nothing logged yet is `[]`, an unknown one a 404.
 */
export const serveJobLogs = async (
  args: { storage: LogStorage; db: DB; jobId: string; kind: LogKind },
): Promise<Response> => {
  const { storage, db, jobId, kind } = args;
  const headers = { "Content-Type": "application/json" };
  const stream = await streamStoredLogs(storage, jobId, kind);
  if (stream) {
    return new Response(stream, { headers });
  }
  const legacy = kind === "build" ? await db.getBuildLogs(jobId) : await db.getRunLogs(jobId);
  if (legacy) {
    return new Response(JSON.stringify(legacy), { headers });
  }
  if (await db.getJobDefinition(jobId)) {
    return new Response("[]", { headers });
  }
  return new Response(JSON.stringify({ error: "Job not found" }), { status: 404, headers });
};

/**
 * The writer: lines in, chunks out, merged when the job finishes. One per
 * queue, on the instance holding the workers' sockets.
 */
export class JobLogs {
  readonly storage: LogStorage;
  private readonly flushInterval: number;
  private readonly maxLines: number;
  /** Lines not written yet, by job and kind */
  private queued = new Map<string, ConsoleLogLine[]>();
  /** Per job and kind, every storage operation chained, so they run one at a time and in order */
  private chains = new Map<string, Promise<void>>();
  /** Jobs that finished: a write for one is merged straight into its log */
  private finished = new LRUMapWithDelete<string, true>(1000);
  /** Names this instance's chunks, so chunks written in the same millisecond by two instances never collide */
  private readonly instance = crypto.randomUUID().slice(0, 8);
  private sequence = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(storage: LogStorage, opts?: { flushInterval?: number; maxLines?: number }) {
    this.storage = storage;
    this.flushInterval = opts?.flushInterval ?? 1000;
    this.maxLines = opts?.maxLines ?? MAX_LOG_LINES;
  }

  /** Queue lines to be written within flushInterval. */
  append(jobId: string, kind: LogKind, lines: ConsoleLogLine[]): void {
    if (!lines.length || !isSafeJobId(jobId)) {
      return;
    }
    const id = key(jobId, kind);
    const queued = this.queued.get(id);
    if (queued) {
      queued.push(...lines);
    } else {
      this.queued.set(id, [...lines]);
    }
    this.scheduleFlush();
  }

  /** Write every queued line now. Settles once they are stored. */
  flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    return Promise.all([...this.queued.keys()].map((id) => {
      const [jobId, kind] = unkey(id);
      return this.chain(jobId, kind, () => this.writeQueued(jobId, kind));
    })).then(() => {});
  }

  /**
   * Start a job's log (every kind if none given) again: drop what is queued
   * and delete what is stored. A worker about to stream a run container's
   * output from the start, or a job running again.
   */
  reset(jobId: string, kind?: LogKind): Promise<void> {
    this.finished.delete(jobId);
    return Promise.all((kind ? [kind] : KINDS).map((k) => {
      // Now, not in the chain: lines appended after this call belong to the new log
      this.queued.delete(key(jobId, k));
      return this.chain(jobId, k, async () => {
        const keys = await storedKeys(this.storage, jobId, k);
        await Promise.all(keys.map((stored) => this.storage.delete(stored)));
      });
    })).then(() => {});
  }

  /**
   * The job finished: write what is queued and merge its chunks into the one
   * log file. Lines arriving after this (a worker still pushing the built
   * image) are merged into it as they are written. Settles once merged.
   */
  finish(jobId: string): Promise<void> {
    this.finished.set(jobId, true);
    return Promise.all(KINDS.map((kind) =>
      this.chain(jobId, kind, async () => {
        await this.writeQueued(jobId, kind);
        await this.merge(jobId, kind);
      })
    )).then(() => {});
  }

  /** The job's log so far: what is stored, then what is not written yet. */
  read(jobId: string, kind: LogKind): Promise<ConsoleLogLine[]> {
    let lines: ConsoleLogLine[] = [];
    // In the chain, so no write or merge of this log is half done meanwhile
    return this.chain(jobId, kind, async () => {
      lines = [...(await readStoredLogs(this.storage, jobId, kind)), ...(this.queued.get(key(jobId, kind)) ?? [])];
    }).then(() => lines);
  }

  /** Forget a job that left the queue. Its stored log stays. */
  forget(jobId: string): void {
    this.finished.delete(jobId);
  }

  private scheduleFlush() {
    if (this.timer) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.flush();
    }, this.flushInterval);
    // A pending flush must not keep a process alive that is otherwise done
    (globalThis as { Deno?: { unrefTimer(id: number): void } }).Deno?.unrefTimer(this.timer as unknown as number);
  }

  /** Write the queued lines as one more chunk (merged, if the job finished). Runs in the chain. */
  private async writeQueued(jobId: string, kind: LogKind): Promise<void> {
    const id = key(jobId, kind);
    const lines = this.queued.get(id);
    if (!lines?.length) {
      return;
    }
    this.queued.delete(id);
    const name = `${String(Date.now()).padStart(15, "0")}-${this.instance}-${
      String(this.sequence++).padStart(9, "0")
    }.json`;
    try {
      await this.storage.put(formatJobLogChunksPrefix(jobId, kind) + name, JSON.stringify(lines));
    } catch (err) {
      // Keep them, ahead of anything queued since, for the next flush
      console.error(`Failed to write ${kind} logs of job ${jobId}, will retry:`, err);
      this.queued.set(id, [...lines, ...(this.queued.get(id) ?? [])]);
      this.scheduleFlush();
      return;
    }
    if (this.finished.has(jobId)) {
      await this.merge(jobId, kind);
    }
  }

  /** Merge the chunks into the one log file, then delete them. Runs in the chain. */
  private async merge(jobId: string, kind: LogKind): Promise<void> {
    const chunks = await this.storage.list(formatJobLogChunksPrefix(jobId, kind));
    if (!chunks.length) {
      return;
    }
    const finalKey = formatJobLogsKey(jobId, kind);
    const bodies = await Promise.all([finalKey, ...chunks].map((stored) => this.storage.get(stored)));
    const lines = bodies.flatMap(parseLines);
    await this.storage.put(
      finalKey,
      JSON.stringify(lines.length > this.maxLines ? lines.slice(-this.maxLines) : lines),
    );
    await Promise.all(chunks.map((chunk) => this.storage.delete(chunk)));
  }

  private chain(jobId: string, kind: LogKind, operation: () => Promise<void>): Promise<void> {
    const id = key(jobId, kind);
    const next = (this.chains.get(id) ?? Promise.resolve()).then(operation).catch((err) => {
      console.error(`Failed to store ${kind} logs of job ${jobId}:`, err);
    });
    this.chains.set(id, next);
    // Drop the entry once settled, unless another operation was chained since
    next.then(() => {
      if (this.chains.get(id) === next) {
        this.chains.delete(id);
      }
    });
    return next;
  }
}

const key = (jobId: string, kind: LogKind) => `${jobId}\n${kind}`;
const unkey = (id: string) => id.split("\n") as [string, LogKind];
