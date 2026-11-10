import { assert, assertEquals, assertRejects, assertStrictEquals } from "std/assert";

import { makeEnsureQueue } from "./ensure-queue.ts";

// Stands in for a job queue: like BaseDockerJobQueue, `db` is only assigned by
// the async setup(), so a caller handed the queue too early sees it undefined.
class FakeQueue {
  db: { getFinishedJob(): string } | undefined;
  disposed = false;
  constructor(public address: string, private failSetup = false) {}
  async setup() {
    await new Promise((resolve) => setTimeout(resolve, 20));
    if (this.failSetup) {
      throw new Error("setup failed");
    }
    this.db = { getFinishedJob: () => "ok" };
  }
  dispose() {
    this.disposed = true;
  }
}

Deno.test("concurrent first calls for a new queue all get the same, set-up queue", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  let created = 0;
  const ensureQueue = makeEnsureQueue(registry, (queue) => {
    created++;
    return new FakeQueue(queue);
  });

  // Use the queue the moment each caller gets it, as a request handler would
  const results = await Promise.all(
    [1, 2, 3, 4].map(() =>
      ensureQueue("freshqueue").then((q) => {
        // The failure in #347: "Cannot read properties of undefined (reading 'getFinishedJob')"
        return { q, finished: q.db?.getFinishedJob() };
      })
    ),
  );

  assertEquals(created, 1);
  const queues = results.map((r) => r.q);
  for (const { q, finished } of results) {
    assertStrictEquals(q, queues[0]);
    assertEquals(finished, "ok");
  }
  assertStrictEquals(registry["freshqueue"], queues[0]);
});

Deno.test("a queue is not published until its setup has finished", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  const ensureQueue = makeEnsureQueue(registry, (queue) => new FakeQueue(queue));

  const pending = ensureQueue("q");
  assertEquals(registry["q"], undefined);
  await pending;
  assert(registry["q"]?.db);
});

Deno.test("a failed setup is retried on the next call rather than cached", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  let attempts = 0;
  const ensureQueue = makeEnsureQueue(registry, (queue) => new FakeQueue(queue, ++attempts === 1));

  const results = await Promise.allSettled([ensureQueue("q"), ensureQueue("q")]);
  assertEquals(results.map((r) => r.status), ["rejected", "rejected"]);
  assertEquals(attempts, 1);
  assertEquals(registry["q"], undefined);

  const q = await ensureQueue("q");
  assertEquals(attempts, 2);
  assertEquals(q.db?.getFinishedJob(), "ok");
});

Deno.test("a synchronous throw from create() is not cached", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  let attempts = 0;
  const ensureQueue = makeEnsureQueue(registry, (queue) => {
    if (++attempts === 1) {
      throw new Error("create failed");
    }
    return new FakeQueue(queue);
  });

  await assertRejects(() => ensureQueue("q"), Error, "create failed");
  const q = await ensureQueue("q");
  assertEquals(q.db?.getFinishedJob(), "ok");
});

Deno.test("a disposed queue (removed from the registry) is rebuilt on the next call", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  const ensureQueue = makeEnsureQueue(registry, (queue) => new FakeQueue(queue));

  const first = await ensureQueue("q");
  assertStrictEquals(await ensureQueue("q"), first);
  // What BaseDockerJobQueue.dispose() does
  delete registry["q"];
  const second = await ensureQueue("q");
  assert(second !== first);
  assertStrictEquals(registry["q"], second);
});

Deno.test("a queue whose setup failed is disposed, not left half-built", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  const created: FakeQueue[] = [];
  const ensureQueue = makeEnsureQueue(registry, (queue) => {
    const q = new FakeQueue(queue, created.length === 0);
    created.push(q);
    return q;
  });

  await assertRejects(() => ensureQueue("q"), Error, "setup failed");
  assertEquals(created.length, 1);
  assert(created[0].disposed);

  const q = await ensureQueue("q");
  assertStrictEquals(q, created[1]);
  assertEquals(q.disposed, false);
  assertStrictEquals(registry["q"], q);
});

Deno.test("extra arguments reach create() for the call that builds the queue", async () => {
  const registry: { [queue: string]: FakeQueue } = {};
  const seen: (string | undefined)[] = [];
  const ensureQueue = makeEnsureQueue(registry, (queue: string, opts?: { tag?: string }) => {
    seen.push(opts?.tag);
    return new FakeQueue(queue);
  });

  const [a, b] = await Promise.all([ensureQueue("q", { tag: "first" }), ensureQueue("q", { tag: "second" })]);
  assertStrictEquals(a, b);
  assertEquals(seen, ["first"]);

  // Once removed (as dispose() does), the next caller's arguments build the new one
  delete registry["q"];
  await ensureQueue("q", { tag: "third" });
  assertEquals(seen, ["first", "third"]);
});
