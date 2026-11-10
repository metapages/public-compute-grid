import { assertEquals } from "std/assert";
import { join } from "std/path";

import { LogSpool } from "./logspool.ts";
import type { ConsoleLogLine } from "./types.ts";

const lines = (prefix: string, n: number): ConsoleLogLine[] =>
  Array.from({ length: n }, (_, i) => [`${prefix}-${i}\n`, 1000 + i]);

const withDir = async (test: (dir: string) => Promise<void>) => {
  const dir = await Deno.makeTempDir();
  try {
    await test(dir);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
};

Deno.test("LogSpool: lines appended survive into a new spool on the same directory", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: dir, maxLines: 100 });
    spool.append("job1", "build", lines("b", 2));
    spool.append("job1", "run", lines("r", 3));
    spool.append("job1", "run", [["stderr\n", 2000, true]]);
    await spool.flush();

    const loaded = await new LogSpool({ directory: dir, maxLines: 100 }).load();
    const byKind = Object.fromEntries(loaded.map((l) => [`${l.jobId}.${l.kind}`, l.lines]));
    assertEquals(byKind["job1.build"], lines("b", 2));
    assertEquals(byKind["job1.run"], [...lines("r", 3), ["stderr\n", 2000, true]]);
  }));

Deno.test("LogSpool: lines are written within the flush interval without an explicit flush", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: dir, maxLines: 100, flushInterval: 10 });
    spool.append("job1", "run", lines("r", 2));
    await new Promise((resolve) => setTimeout(resolve, 200));
    const loaded = await new LogSpool({ directory: dir, maxLines: 100 }).load();
    assertEquals(loaded[0]?.lines, lines("r", 2));
  }));

Deno.test("LogSpool: clear drops pending lines and the files", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: dir, maxLines: 100 });
    spool.append("job1", "run", lines("r", 2));
    spool.append("job1", "build", lines("b", 2));
    await spool.flush();
    spool.append("job1", "run", lines("late", 2));
    await spool.clear("job1", "run");
    await spool.flush();
    const loaded = await spool.load();
    assertEquals(loaded.map((l) => l.kind), ["build"]);

    await spool.clear("job1");
    assertEquals(await spool.load(), []);
  }));

Deno.test("LogSpool: a line torn by a kill is skipped, the lines around it kept", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: dir, maxLines: 100 });
    spool.append("job1", "run", lines("before", 2));
    await spool.flush();
    // What a process killed mid-write leaves: half a line, no newline
    await Deno.writeTextFile(join(dir, "job1.run.jsonl"), '\n["torn', { append: true });
    spool.append("job1", "run", lines("after", 2));
    await spool.flush();
    const loaded = await spool.load();
    assertEquals(loaded[0].lines, [...lines("before", 2), ...lines("after", 2)]);
  }));

Deno.test("LogSpool: a runaway job's file is compacted to its tail", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: dir, maxLines: 10 });
    const all = lines("r", 35);
    for (const line of all) {
      spool.append("job1", "run", [line]);
      await spool.flush();
    }
    const fileLines = (await Deno.readTextFile(join(dir, "job1.run.jsonl"))).split("\n").filter(Boolean);
    assertEquals(fileLines.length <= 20, true, `file holds ${fileLines.length} lines`);
    const loaded = await spool.load();
    assertEquals(loaded[0].lines, all.slice(-10));
  }));

Deno.test("LogSpool: job ids cannot escape the directory", () =>
  withDir(async (dir) => {
    const spool = new LogSpool({ directory: join(dir, "q"), maxLines: 10 });
    spool.append("../../evil", "run", lines("r", 1));
    await spool.flush();
    assertEquals([...Deno.readDirSync(dir)].map((e) => e.name), ["q"]);
  }));
