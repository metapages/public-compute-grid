/**
 * The key-value store the queue persists into, as a runtime-neutral interface.
 *
 * It is the subset of Deno.Kv the code actually uses, shaped so a Deno.Kv
 * satisfies it structurally (local mode and the Deno server keep using Deno KV
 * unchanged), while SqlKvStore implements it over SQLite for Cloudflare (D1,
 * Durable Object storage) and Node (node:sqlite).
 *
 * Constraints the implementations rely on, so keep callers to them:
 * - key parts are strings
 * - values are plain JSON (no Date, Map, Uint8Array, bigint)
 */

export type KvKeyPart = string;
export type KvKey = readonly KvKeyPart[];

export interface KvEntryMaybe<T> {
  key: KvKey;
  value: T | null;
  versionstamp: string | null;
}

export interface KvEntry<T> {
  key: KvKey;
  value: T;
  versionstamp: string;
}

export interface KvSetOptions {
  /** Milliseconds until the entry expires and stops being returned */
  expireIn?: number;
}

export interface KvCommitResult {
  ok: true;
  versionstamp: string;
}

export interface KvCommitError {
  ok: false;
}

export interface KvAtomicCheck {
  key: KvKey;
  /** null asserts the key does not exist */
  versionstamp: string | null;
}

export interface KvAtomicOperation {
  check(...checks: KvAtomicCheck[]): this;
  set(key: KvKey, value: unknown, options?: KvSetOptions): this;
  delete(key: KvKey): this;
  commit(): Promise<KvCommitResult | KvCommitError>;
}

export interface KvListSelector {
  prefix: KvKey;
}

export interface KvListOptions {
  limit?: number;
  /** Deno KV only; SQL stores are always strongly consistent */
  consistency?: "strong" | "eventual";
}

export interface KvStore {
  get<T = unknown>(key: KvKey): Promise<KvEntryMaybe<T>>;
  set(key: KvKey, value: unknown, options?: KvSetOptions): Promise<KvCommitResult>;
  delete(key: KvKey): Promise<void>;
  list<T = unknown>(selector: KvListSelector, options?: KvListOptions): AsyncIterable<KvEntry<T>>;
  atomic(): KvAtomicOperation;
  close(): void;
}
