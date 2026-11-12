import { assertEquals } from "std/assert";

import type { DB } from "./db.ts";
import {
  DiskLogStorage,
  formatJobLogChunksPrefix,
  formatJobLogsKey,
  JobLogs,
  readStoredLogs,
  serveJobLogs,
  streamStoredLogs,
} from "./joblogs.ts";
import { parseListObjectsV2 } from "./s3.ts";
import { MemoryLogStorage } from "./joblogs_memory.ts";
import type { ConsoleLogLine } from "./types.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
let counter = 0;
const lines = (n: number): ConsoleLogLine[] => Array.from({ length: n }, () => [`line-${counter++}\n`, Date.now()]);
const texts = (logs: ConsoleLogLine[]) => logs.map((line) => line[0]);

const chunkKeys = (storage: MemoryLogStorage, jobId: string, kind: "build" | "run") =>
  [...storage.objects.keys()].filter((key) => key.startsWith(formatJobLogChunksPrefix(jobId, kind)));

const opts = { sanitizeOps: false, sanitizeResources: false };

Deno.test("JobLogs: lines are written to storage within the flush interval, as chunks", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 10 });
  const first = lines(3);
  const second = lines(2);
  logs.append("job1", "run", first);
  await sleep(100);
  logs.append("job1", "run", second);
  await sleep(100);
  assertEquals(chunkKeys(storage, "job1", "run").length, 2);
  // What another instance (or a restarted server) reads: storage alone
  assertEquals(texts(await readStoredLogs(storage, "job1", "run")), texts([...first, ...second]));
});

Deno.test("JobLogs: read includes the lines not written yet", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000 });
  const written = lines(3);
  logs.append("job1", "build", written);
  await logs.flush();
  const queued = lines(2);
  logs.append("job1", "build", queued);
  assertEquals(texts(await logs.read("job1", "build")), texts([...written, ...queued]));
  assertEquals(texts(await readStoredLogs(storage, "job1", "build")), texts(written));
});

Deno.test("JobLogs: finish merges the chunks into one file, and later lines into it", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 5 });
  const before = [lines(4), lines(4), lines(4)];
  for (const batch of before) {
    logs.append("job1", "run", batch);
    await sleep(20);
  }
  const unwritten = lines(3);
  logs.append("job1", "run", unwritten);
  await logs.finish("job1");

  assertEquals(chunkKeys(storage, "job1", "run"), []);
  const merged = JSON.parse(storage.objects.get(formatJobLogsKey("job1", "run"))!);
  assertEquals(texts(merged), texts([...before.flat(), ...unwritten]));

  // A worker still pushing the built image after the job finished
  const late = lines(2);
  logs.append("job1", "run", late);
  await logs.flush();
  assertEquals(chunkKeys(storage, "job1", "run"), []);
  assertEquals(
    texts(JSON.parse(storage.objects.get(formatJobLogsKey("job1", "run"))!)),
    texts([...before.flat(), ...unwritten, ...late]),
  );
});

Deno.test("JobLogs: lines arriving while the merge is being written are kept, once", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 5 });
  storage.delay = 20;
  const before = lines(10);
  logs.append("job1", "run", before);
  const finishing = logs.finish("job1");
  await sleep(10);
  const during = lines(5);
  logs.append("job1", "run", during);
  await finishing;
  await logs.flush();
  assertEquals(texts(await readStoredLogs(storage, "job1", "run")), texts([...before, ...during]));
});

Deno.test("JobLogs: reset deletes what is stored and queued", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000 });
  logs.append("job1", "run", lines(3));
  logs.append("job1", "build", lines(3));
  await logs.finish("job1");
  logs.append("job1", "run", lines(3));
  await logs.reset("job1", "run");
  assertEquals(await logs.read("job1", "run"), []);
  assertEquals((await logs.read("job1", "build")).length, 3);

  const fresh = lines(2);
  logs.append("job1", "run", fresh);
  await logs.flush();
  // Not finished any more: a fresh run's lines stay chunks until it finishes
  assertEquals(chunkKeys(storage, "job1", "run").length, 1);
  assertEquals(texts(await logs.read("job1", "run")), texts(fresh));

  await logs.reset("job1");
  assertEquals([...storage.objects.keys()], []);
});

Deno.test("JobLogs: a failed write is retried, in order", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 5 });
  storage.failPuts = 2;
  const first = lines(3);
  logs.append("job1", "run", first);
  await logs.flush();
  const second = lines(3);
  logs.append("job1", "run", second);
  await sleep(200);
  assertEquals(texts(await readStoredLogs(storage, "job1", "run")), texts([...first, ...second]));
});

Deno.test("JobLogs: the merged log is capped to its tail", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000, maxLines: 10 });
  const all = lines(25);
  logs.append("job1", "run", all);
  await logs.finish("job1");
  assertEquals(texts(await readStoredLogs(storage, "job1", "run")), texts(all.slice(-10)));
});

Deno.test("JobLogs: job ids that are not ids have no logs", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000 });
  logs.append("../../etc", "run", lines(1));
  await logs.flush();
  assertEquals([...storage.objects.keys()], []);
  assertEquals(await readStoredLogs(storage, "../x", "run"), []);
});

Deno.test("streamStoredLogs: the merged file and the chunks as one JSON array", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000 });
  const merged = lines(3);
  logs.append("job1", "run", merged);
  await logs.finish("job1");
  // An empty chunk too: it must not leave a stray comma
  await storage.put(`${formatJobLogChunksPrefix("job1", "run")}000000000000001-a-0.json`, "[]");
  const chunk = lines(2);
  await storage.put(`${formatJobLogChunksPrefix("job1", "run")}999999999999999-a-0.json`, JSON.stringify(chunk));

  const stream = await streamStoredLogs(storage, "job1", "run");
  const text = await new Response(stream).text();
  assertEquals(texts(JSON.parse(text)), texts([...merged, ...chunk]));
  assertEquals(await streamStoredLogs(storage, "nothing", "run"), undefined);
});

Deno.test("serveJobLogs: stored, legacy, known without logs, unknown", opts, async () => {
  const storage = new MemoryLogStorage();
  const logs = new JobLogs(storage, { flushInterval: 60_000 });
  const stored = lines(2);
  logs.append("stored", "build", stored);
  await logs.flush();
  const legacy = lines(2);
  const db = {
    getBuildLogs: (jobId: string) => Promise.resolve(jobId === "legacy" ? legacy : null),
    getRunLogs: () => Promise.resolve(null),
    getJobDefinition: (jobId: string) => Promise.resolve(jobId === "known" ? { image: "alpine" } : undefined),
  } as unknown as DB;

  const get = async (jobId: string) => {
    const response = await serveJobLogs({ storage, db, jobId, kind: "build" });
    return { status: response.status, body: await response.json() };
  };
  assertEquals(texts((await get("stored")).body), texts(stored));
  assertEquals(texts((await get("legacy")).body), texts(legacy));
  assertEquals(await get("known"), { status: 200, body: [] });
  assertEquals((await get("unknown")).status, 404);
});

Deno.test("DiskLogStorage: put, get, list in order, delete", async () => {
  const dir = await Deno.makeTempDir();
  try {
    const storage = new DiskLogStorage(dir);
    await storage.put("j/a/logs/run/2.json", "[2]");
    await storage.put("j/a/logs/run/1.json", "[1]");
    await storage.put("j/a/logs/run.json", "[0]");
    assertEquals(await storage.get("j/a/logs/run/1.json"), "[1]");
    assertEquals(await storage.get("j/a/logs/missing.json"), undefined);
    assertEquals(await storage.list("j/a/logs/run/"), ["j/a/logs/run/1.json", "j/a/logs/run/2.json"]);
    assertEquals(await storage.list("j/missing/logs/run/"), []);
    await storage.delete("j/a/logs/run/1.json");
    await storage.delete("j/a/logs/run/1.json");
    assertEquals(await storage.list("j/a/logs/run/"), ["j/a/logs/run/2.json"]);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
});

Deno.test("parseListObjectsV2: keys and the next page", () => {
  const page = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>b</Name><Prefix>j/a/logs/run/</Prefix>
<Contents><Key>j/a/logs/run/1.json</Key><Size>3</Size></Contents>
<Contents><Key>j/a/logs/run/2&amp;3.json</Key><Size>3</Size></Contents>
<IsTruncated>true</IsTruncated><NextContinuationToken>abc&amp;=</NextContinuationToken></ListBucketResult>`;
  assertEquals(parseListObjectsV2(page), {
    keys: ["j/a/logs/run/1.json", "j/a/logs/run/2&3.json"],
    nextContinuationToken: "abc&=",
  });
  assertEquals(parseListObjectsV2("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"), {
    keys: [],
    nextContinuationToken: undefined,
  });
});
