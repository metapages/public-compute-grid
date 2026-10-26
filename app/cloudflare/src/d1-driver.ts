import type { SqlDriver, SqlStatement, SqlValue } from "@shared/store/sql-kv.ts";

// The slice of the D1 binding this driver uses (see @cloudflare/workers-types)
interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  all<T = Record<string, unknown>>(): Promise<{ results: T[] }>;
}
export interface D1Database {
  prepare(query: string): D1PreparedStatement;
  batch<T = Record<string, unknown>>(statements: D1PreparedStatement[]): Promise<{ results: T[] }[]>;
}

/**
 * SqlDriver over Cloudflare D1. A D1 batch runs as one SQL transaction: if any
 * statement fails the whole batch rolls back, which is what SqlKvStore's
 * atomic checks rely on.
 */
export class D1Driver implements SqlDriver {
  private readonly db: D1Database;

  constructor(db: D1Database) {
    this.db = db;
  }

  async batch(statements: SqlStatement[]): Promise<Record<string, SqlValue>[][]> {
    const prepared = statements.map((s) => this.db.prepare(s.sql).bind(...s.params));
    if (prepared.length === 1) {
      const { results } = await prepared[0].all<Record<string, SqlValue>>();
      return [results ?? []];
    }
    const results = await this.db.batch<Record<string, SqlValue>>(prepared);
    return results.map((r) => r.results ?? []);
  }
}
