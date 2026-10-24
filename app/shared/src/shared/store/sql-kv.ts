import type {
  KvAtomicCheck,
  KvAtomicOperation,
  KvCommitError,
  KvCommitResult,
  KvEntry,
  KvEntryMaybe,
  KvKey,
  KvListOptions,
  KvListSelector,
  KvSetOptions,
  KvStore,
} from "./kv.ts";

/**
 * KvStore over SQLite. One implementation for every SQLite we deploy on:
 * Cloudflare D1, Durable Object storage, and node:sqlite (Node and Deno) —
 * each supplies a SqlDriver.
 */

export type SqlValue = string | number | null;

export interface SqlStatement {
  sql: string;
  params: SqlValue[];
}

export interface SqlDriver {
  /**
   * Run the statements as one transaction, in order. If any statement fails,
   * none of them take effect and the promise rejects. Resolves with the rows
   * each statement returned.
   */
  batch(statements: SqlStatement[]): Promise<Record<string, SqlValue>[][]>;
}

export const SQL_KV_SCHEMA: SqlStatement[] = [
  {
    sql: `CREATE TABLE IF NOT EXISTS kv (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      version TEXT NOT NULL,
      expires_at INTEGER
    )`,
    params: [],
  },
  { sql: `CREATE INDEX IF NOT EXISTS kv_expires_at ON kv (expires_at)`, params: [] },
  // A failed atomic check inserts a 0 here, violating the constraint, which
  // aborts the whole transaction. This is how a check is made atomic with the
  // writes in drivers that only offer non-interactive batches (D1).
  { sql: `CREATE TABLE IF NOT EXISTS kv_check (ok INTEGER NOT NULL CHECK (ok = 1))`, params: [] },
];

// Key parts are URI-encoded so they never contain the separator, and joined
// with the ASCII unit separator. Everything under a prefix then sorts between
// `<prefix>\x1f` and `<prefix>\x20`.
const SEPARATOR = "\x1f";
const AFTER_SEPARATOR = "\x20";

export const encodeKey = (key: KvKey): string => key.map((part) => encodeURIComponent(part)).join(SEPARATOR);

export const decodeKey = (encoded: string): KvKey => encoded.split(SEPARATOR).map((part) => decodeURIComponent(part));

const newVersionstamp = (): string => `${Date.now().toString(16).padStart(12, "0")}${crypto.randomUUID()}`;

const LIVE = `(expires_at IS NULL OR expires_at > ?)`;

const upsert = (key: KvKey, value: unknown, version: string, now: number, options?: KvSetOptions): SqlStatement => ({
  sql: `INSERT INTO kv (key, value, version, expires_at) VALUES (?, ?, ?, ?)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value, version = excluded.version, expires_at = excluded.expires_at`,
  params: [
    encodeKey(key),
    JSON.stringify(value),
    version,
    options?.expireIn !== undefined ? now + options.expireIn : null,
  ],
});

const remove = (key: KvKey): SqlStatement => ({
  sql: `DELETE FROM kv WHERE key = ?`,
  params: [encodeKey(key)],
});

const isCheckFailure = (err: unknown): boolean => String((err as Error)?.message ?? err).includes("CHECK constraint");

const LIST_PAGE_SIZE = 500;

export class SqlKvStore implements KvStore {
  private readonly driver: SqlDriver;
  private ready: Promise<void> | undefined;

  constructor(driver: SqlDriver) {
    this.driver = driver;
  }

  /** Create the tables. Safe to call repeatedly; every operation awaits it. */
  init(): Promise<void> {
    if (!this.ready) {
      this.ready = this.driver.batch(SQL_KV_SCHEMA).then(() => undefined);
    }
    return this.ready;
  }

  private async run(statements: SqlStatement[]): Promise<Record<string, SqlValue>[][]> {
    await this.init();
    return this.driver.batch(statements);
  }

  async get<T = unknown>(key: KvKey): Promise<KvEntryMaybe<T>> {
    const [rows] = await this.run([{
      sql: `SELECT value, version FROM kv WHERE key = ? AND ${LIVE}`,
      params: [encodeKey(key), Date.now()],
    }]);
    const row = rows[0];
    if (!row) {
      return { key, value: null, versionstamp: null };
    }
    return { key, value: JSON.parse(row.value as string) as T, versionstamp: row.version as string };
  }

  async set(key: KvKey, value: unknown, options?: KvSetOptions): Promise<KvCommitResult> {
    const versionstamp = newVersionstamp();
    await this.run([upsert(key, value, versionstamp, Date.now(), options)]);
    return { ok: true, versionstamp };
  }

  async delete(key: KvKey): Promise<void> {
    await this.run([remove(key)]);
  }

  async *list<T = unknown>(selector: KvListSelector, options?: KvListOptions): AsyncIterable<KvEntry<T>> {
    const prefix = encodeKey(selector.prefix);
    const lower = selector.prefix.length > 0 ? prefix + SEPARATOR : "";
    const upper = selector.prefix.length > 0 ? prefix + AFTER_SEPARATOR : undefined;
    let remaining = options?.limit ?? Infinity;
    let after: string | undefined;

    while (remaining > 0) {
      const pageSize = Math.min(LIST_PAGE_SIZE, remaining);
      const conditions = [after === undefined ? `key >= ?` : `key > ?`, LIVE];
      const params: SqlValue[] = [after ?? lower, Date.now()];
      if (upper !== undefined) {
        conditions.push(`key < ?`);
        params.push(upper);
      }
      params.push(pageSize);
      const [rows] = await this.run([{
        sql: `SELECT key, value, version FROM kv WHERE ${conditions.join(" AND ")} ORDER BY key LIMIT ?`,
        params,
      }]);
      for (const row of rows) {
        yield {
          key: decodeKey(row.key as string),
          value: JSON.parse(row.value as string) as T,
          versionstamp: row.version as string,
        };
      }
      remaining -= rows.length;
      if (rows.length < pageSize) {
        return;
      }
      after = rows[rows.length - 1].key as string;
    }
  }

  atomic(): KvAtomicOperation {
    return new SqlAtomicOperation(this);
  }

  /** @internal used by SqlAtomicOperation */
  async _commit(
    checks: KvAtomicCheck[],
    mutations: ((version: string, now: number) => SqlStatement)[],
  ): Promise<KvCommitResult | KvCommitError> {
    const versionstamp = newVersionstamp();
    const now = Date.now();
    const statements: SqlStatement[] = checks.map((check) =>
      check.versionstamp === null
        ? {
          sql: `INSERT INTO kv_check (ok) SELECT 1 - COUNT(*) FROM kv WHERE key = ? AND ${LIVE}`,
          params: [encodeKey(check.key), now],
        }
        : {
          sql: `INSERT INTO kv_check (ok) SELECT COUNT(*) FROM kv WHERE key = ? AND version = ? AND ${LIVE}`,
          params: [encodeKey(check.key), check.versionstamp, now],
        }
    );
    if (statements.length > 0) {
      statements.push({ sql: `DELETE FROM kv_check`, params: [] });
    }
    statements.push(...mutations.map((m) => m(versionstamp, now)));
    try {
      await this.run(statements);
    } catch (err) {
      if (isCheckFailure(err)) {
        return { ok: false };
      }
      throw err;
    }
    return { ok: true, versionstamp };
  }

  /** Delete expired entries. Reads already ignore them; this reclaims space. */
  async deleteExpired(): Promise<void> {
    await this.run([{ sql: `DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?`, params: [Date.now()] }]);
  }

  close(): void {}
}

class SqlAtomicOperation implements KvAtomicOperation {
  private readonly store: SqlKvStore;
  private readonly checks: KvAtomicCheck[] = [];
  private readonly mutations: ((version: string, now: number) => SqlStatement)[] = [];

  constructor(store: SqlKvStore) {
    this.store = store;
  }

  check(...checks: KvAtomicCheck[]): this {
    this.checks.push(...checks);
    return this;
  }

  set(key: KvKey, value: unknown, options?: KvSetOptions): this {
    this.mutations.push((version, now) => upsert(key, value, version, now, options));
    return this;
  }

  delete(key: KvKey): this {
    this.mutations.push(() => remove(key));
    return this;
  }

  commit(): Promise<KvCommitResult | KvCommitError> {
    return this.store._commit(this.checks, this.mutations);
  }
}
