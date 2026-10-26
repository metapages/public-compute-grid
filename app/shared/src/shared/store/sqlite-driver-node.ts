import { DatabaseSync } from "node:sqlite";

import type { SqlDriver, SqlStatement, SqlValue } from "./sql-kv.ts";

/**
 * SqlDriver over node:sqlite, which both Node and Deno provide. Used for tests,
 * and the intended store for the Node server/worker.
 */
export class NodeSqliteDriver implements SqlDriver {
  private readonly db: DatabaseSync;

  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
  }

  batch(statements: SqlStatement[]): Promise<Record<string, SqlValue>[][]> {
    this.db.exec("BEGIN");
    try {
      const results = statements.map((s) => this.db.prepare(s.sql).all(...s.params) as Record<string, SqlValue>[]);
      this.db.exec("COMMIT");
      return Promise.resolve(results);
    } catch (err) {
      this.db.exec("ROLLBACK");
      return Promise.reject(err);
    }
  }

  close(): void {
    this.db.close();
  }
}
