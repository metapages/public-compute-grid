/**
 * KvStore conformance: the same behaviour from Deno KV (what runs today) and
 * SqlKvStore (what runs on Cloudflare and Node). If both pass, db.ts can't tell
 * them apart.
 */
import { assert, assertEquals } from "std/assert";

import type { KvEntry, KvStore } from "./kv.ts";
import { SqlKvStore } from "./sql-kv.ts";
import { NodeSqliteDriver } from "./sqlite-driver-node.ts";

const implementations: { name: string; open: () => Promise<KvStore> }[] = [
  { name: "deno-kv", open: async () => await Deno.openKv(":memory:") as unknown as KvStore },
  {
    name: "sql-kv (node:sqlite)",
    open: async () => {
      const store = new SqlKvStore(new NodeSqliteDriver());
      await store.init();
      return store;
    },
  },
];

const collect = async <T>(iter: AsyncIterable<KvEntry<T>>): Promise<KvEntry<T>[]> => {
  const out: KvEntry<T>[] = [];
  for await (const entry of iter) out.push(entry);
  return out;
};

for (const { name, open } of implementations) {
  Deno.test(`kv store [${name}]`, async (t) => {
    const kv = await open();
    try {
      await t.step("get missing", async () => {
        const entry = await kv.get(["nope"]);
        assertEquals(entry.value, null);
        assertEquals(entry.versionstamp, null);
      });

      await t.step("set / get / overwrite / delete", async () => {
        const result = await kv.set(["job", "a"], { x: 1, nested: ["y"] });
        assert(result.ok);
        let entry = await kv.get(["job", "a"]);
        assertEquals(entry.value, { x: 1, nested: ["y"] });
        assertEquals(entry.versionstamp, result.versionstamp);

        const second = await kv.set(["job", "a"], "replaced");
        entry = await kv.get(["job", "a"]);
        assertEquals(entry.value, "replaced");
        assert(second.versionstamp !== result.versionstamp);

        await kv.delete(["job", "a"]);
        assertEquals((await kv.get(["job", "a"])).value, null);
        await kv.delete(["job", "a"]); // deleting a missing key is fine
      });

      await t.step("booleans, false and arrays round-trip", async () => {
        await kv.set(["b", "t"], true);
        await kv.set(["b", "f"], false);
        await kv.set(["b", "arr"], [["line", 123, true]]);
        assertEquals((await kv.get(["b", "t"])).value, true);
        assertEquals((await kv.get(["b", "f"])).value, false);
        assertEquals((await kv.get(["b", "arr"])).value, [["line", 123, true]]);
      });

      await t.step("list by prefix: only children, ordered, not the prefix key or siblings", async () => {
        await kv.set(["queue", "q1"], "the prefix key itself");
        await kv.set(["queue", "q1", "job2"], 2);
        await kv.set(["queue", "q1", "job1"], 1);
        await kv.set(["queue", "q1", "job3", "deeper"], 3);
        await kv.set(["queue", "q10", "job9"], 9); // q10 shares a string prefix with q1
        await kv.set(["queue", "q2", "job1"], 21);

        const entries = await collect(kv.list<number>({ prefix: ["queue", "q1"] }));
        assertEquals(entries.map((e) => e.key), [
          ["queue", "q1", "job1"],
          ["queue", "q1", "job2"],
          ["queue", "q1", "job3", "deeper"],
        ]);
        assertEquals(entries.map((e) => e.value), [1, 2, 3]);
        assert(entries.every((e) => typeof e.versionstamp === "string"));

        const limited = await collect(kv.list({ prefix: ["queue", "q1"] }, { limit: 2 }));
        assertEquals(limited.length, 2);
      });

      await t.step("keys with separators and unicode", async () => {
        const odd = ["k", "a/b:c", "spaces and %20", "émoji 🐳", ""];
        await kv.set(odd, "ok");
        assertEquals((await kv.get(odd)).value, "ok");
        const listed = await collect(kv.list({ prefix: ["k", "a/b:c"] }));
        assertEquals(listed.map((e) => e.key), [odd]);
      });

      await t.step("expireIn hides the entry once expired", async () => {
        await kv.set(["exp", "soon"], "x", { expireIn: 1 });
        await kv.set(["exp", "later"], "y", { expireIn: 60_000 });
        await new Promise((r) => setTimeout(r, 50));
        // Deno KV expiry is lazy and not guaranteed to be immediate; the SQL
        // store is exact. db.ts never depends on prompt expiry, only on
        // eventually-gone, so only check the not-yet-expired entry strictly.
        assertEquals((await kv.get(["exp", "later"])).value, "y");
        if (name !== "deno-kv") {
          assertEquals((await kv.get(["exp", "soon"])).value, null);
          assertEquals((await collect(kv.list({ prefix: ["exp"] }))).map((e) => e.key), [["exp", "later"]]);
        }
      });

      await t.step("atomic: commits all operations together", async () => {
        const result = await kv.atomic()
          .set(["at", "1"], "one")
          .set(["at", "2"], "two")
          .delete(["at", "missing"])
          .commit();
        assert(result.ok);
        assertEquals((await kv.get(["at", "1"])).value, "one");
        assertEquals((await kv.get(["at", "2"])).value, "two");
        assertEquals((await kv.get(["at", "1"])).versionstamp, result.versionstamp);
      });

      await t.step("atomic: check against an absent key", async () => {
        const created = await kv.atomic().check({ key: ["chk", "new"], versionstamp: null }).set(["chk", "new"], 1)
          .commit();
        assert(created.ok);
        const again = await kv.atomic().check({ key: ["chk", "new"], versionstamp: null }).set(["chk", "new"], 2)
          .commit();
        assertEquals(again.ok, false);
        assertEquals((await kv.get(["chk", "new"])).value, 1);
      });

      await t.step("atomic: optimistic append (the db.ts job history pattern)", async () => {
        const key = ["job-queue-history", "q", "job"];
        const append = async (item: string) => {
          const current = await kv.get<string[]>(key);
          return await kv.atomic()
            .check(current.value ? current : { key, versionstamp: null })
            .set(key, [...(current.value || []), item])
            .commit();
        };
        assert((await append("a")).ok);
        assert((await append("b")).ok);
        assertEquals((await kv.get(key)).value, ["a", "b"]);

        // A writer holding a stale read loses, and nothing it wrote applies
        const stale = await kv.get<string[]>(key);
        assert((await append("c")).ok);
        const lost = await kv.atomic()
          .check(stale)
          .set(key, [...stale.value!, "stale"])
          .set(["side", "effect"], "should not exist")
          .commit();
        assertEquals(lost.ok, false);
        assertEquals((await kv.get(key)).value, ["a", "b", "c"]);
        assertEquals((await kv.get(["side", "effect"])).value, null);
      });
    } finally {
      kv.close();
    }
  });
}
