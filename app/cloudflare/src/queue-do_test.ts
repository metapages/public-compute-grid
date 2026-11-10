/**
 * Run with the test import map, which stands in for `cloudflare:workers`:
 * `just app/cloudflare/test`.
 */
import { assert, assertEquals, assertNotEquals } from "std/assert";

import { getServerInstanceId } from "@/util/id.ts";
import { setKv } from "@shared/kv.ts";
import type { KvStore } from "@shared/store/kv.ts";

import { QueueDurableObjectBase, type QueueDurableObjectEnv } from "./queue-do.ts";

setKv(await Deno.openKv(":memory:") as unknown as KvStore);

/** One Durable Object instance: the storage survives an eviction, the instance does not. */
class TestQueueDurableObject extends QueueDurableObjectBase<QueueDurableObjectEnv> {
  queueFor(name?: string) {
    return this.ensureQueue(name);
  }
}

const storage = () => {
  const values = new Map<string, unknown>();
  return {
    get: <T>(key: string) => Promise.resolve(values.get(key) as T | undefined),
    put: (key: string, value: unknown) => {
      values.set(key, value);
      return Promise.resolve();
    },
    getAlarm: () => Promise.resolve(null),
    setAlarm: () => Promise.resolve(),
    deleteAlarm: () => Promise.resolve(),
  };
};

const instantiate = (state: { storage: ReturnType<typeof storage> }) =>
  new TestQueueDurableObject(
    { ...state, getWebSockets: () => [], acceptWebSocket: () => {} } as never,
    {} as QueueDurableObjectEnv,
  );

const queueInfo = async (object: TestQueueDurableObject, name?: string) => {
  const queue = await object.queueFor(name);
  assert(queue);
  const status = await queue.status() as { queueInfo: { serverId: string; instanceStartedAt: number } };
  return status.queueInfo;
};

Deno.test("a queue rebuilt after an eviction gets a new serverId, though the isolate's id is unchanged", async () => {
  const isolateId = getServerInstanceId();
  const state = { storage: storage() };
  const queueName = `queue-do-test-${crypto.randomUUID()}`;

  const first = instantiate(state);
  const before = await queueInfo(first, queueName);
  // Resident: the same instance keeps its queue and its id.
  assertEquals(await queueInfo(first), before);
  assertNotEquals(before.serverId, isolateId);

  // Evicted and rebuilt in the same (warm) isolate: a new object instance, the
  // same storage, and no request to name the queue (as for an alarm).
  await new Promise((resolve) => setTimeout(resolve, 2));
  const second = instantiate(state);
  const after = await queueInfo(second);

  assertEquals(getServerInstanceId(), isolateId);
  assertNotEquals(after.serverId, before.serverId);
  assertNotEquals(after.serverId, isolateId);
  assert(
    after.instanceStartedAt > before.instanceStartedAt,
    `instanceStartedAt ${after.instanceStartedAt} should be after ${before.instanceStartedAt}`,
  );
});
