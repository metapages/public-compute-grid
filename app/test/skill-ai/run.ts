#!/usr/bin/env -S deno run --allow-all
/**
 * LLM-in-the-loop harness for the `compute-queues` skill.
 *
 * Spawns a headless `claude -p` session with the skill installed and a prompt,
 * lets it drive the real API on a real stack, then verifies the OUTCOME rather
 * than the transcript: a job actually ran on the queue, it exited 0, and it
 * produced the artifacts the scenario demanded.
 *
 * This is the only test that answers "does the skill text actually lead a model
 * to a working container?" — the deterministic suite
 * (app/test/src/skill_compute_queues_test.ts) covers the endpoints beneath
 * it. It costs tokens and is not deterministic, so it is not part of `just
 * test`.
 *
 *   deno run --allow-all app/test/skill-ai/run.ts
 *   deno run --allow-all app/test/skill-ai/run.ts --scenario hello --keep
 *
 * Requires `claude` on PATH and a worker on the queue. A local-mode worker
 * serves the API itself, so that alone is enough and starts in seconds:
 *
 *   just app/worker/local          # API on :8000, queue "local"
 */
import { parseArgs } from "@std/cli/parse-args";
import { dirname, fromFileUrl, join, resolve } from "std/path";

const HERE = dirname(fromFileUrl(import.meta.url));
const REPO_ROOT = resolve(HERE, "../../..");
const SKILL_SRC = join(REPO_ROOT, "docs/public/skill/compute-queues");

interface Scenario {
  name: string;
  description: string;
  prompt: (ctx: { api: string; queue: string; workdir: string }) => string;
  /** Throws with a readable message if the scenario was not actually achieved. */
  verify: (ctx: { api: string; queue: string; workdir: string; before: Set<string> }) => Promise<void>;
}

// ---------------------------------------------------------------------------
// helpers shared by scenarios
// ---------------------------------------------------------------------------

interface QueueStatus {
  jobs?: Record<string, { state: string }>;
  localWorkers?: Record<string, unknown>;
  otherServers?: Record<string, unknown>;
}

const getJson = async (url: string) => {
  const res = await fetch(url, { redirect: "follow" });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return await res.json();
};

const queueStatus = (api: string, queue: string): Promise<QueueStatus> => getJson(`${api}/q/${queue}/status`);

const jobsOnQueue = async (api: string, queue: string): Promise<string[]> => {
  const status = await queueStatus(api, queue).catch(() => undefined);
  return Object.keys(status?.jobs || {});
};

// deno-lint-ignore no-explicit-any
const jobResult = async (api: string, queue: string, jobId: string): Promise<any> => {
  const { data } = await getJson(`${api}/q/${queue}/j/${jobId}/result.json`);
  return data;
};

/**
 * The scenario passes if ANY job the session created satisfies `check` — the
 * job completed, the program exited 0, AND the artifacts are right.
 *
 * Checking every candidate rather than the first exit-0 one matters: the skill
 * is an iteration loop, so a session routinely leaves behind earlier attempts,
 * and an attempt can exit 0 while still being wrong (a mangled shell command
 * whose failure the shell swallows writes an empty output file and returns 0).
 * Grading the first such job marks a session failed that in fact recovered and
 * finished correctly.
 *
 * Only jobs that appeared during the run count. A worker subscribes to one
 * queue, so runs share a queue and it may already hold unrelated jobs.
 */
const verifyAnyJob = async (
  api: string,
  queue: string,
  before: Set<string>,
  // deno-lint-ignore no-explicit-any
  check: (jobId: string, result: any) => Promise<void>,
): Promise<void> => {
  const jobIds = (await jobsOnQueue(api, queue)).filter((id) => !before.has(id));
  if (!jobIds.length) {
    throw new Error(`no new jobs appeared on ${queue} — the session never ran anything`);
  }

  const attempts: string[] = [];
  for (const jobId of jobIds) {
    const result = await jobResult(api, queue, jobId);
    const reason = result?.finishedReason;
    const code = result?.finished?.result?.StatusCode;
    const label = `  ${jobId.slice(0, 12)} ${reason ?? result?.state ?? "?"} StatusCode=${code ?? "-"}`;
    if (reason !== "Success" || code !== 0) {
      attempts.push(label);
      continue;
    }
    try {
      await check(jobId, result);
      return;
    } catch (err) {
      attempts.push(`${label} — ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  throw new Error(
    `${jobIds.length} job(s) ran on ${queue}, none satisfied the scenario:\n${attempts.join("\n")}`,
  );
};

const assertOutputs = (
  // deno-lint-ignore no-explicit-any
  result: any,
  required: string[],
) => {
  const outputs = Object.keys(result?.finished?.result?.outputs || {});
  const missing = required.filter((r) => !outputs.includes(r));
  if (missing.length) {
    throw new Error(
      `job succeeded but is missing required output(s) ${missing.join(", ")}; got: ${outputs.join(", ") || "none"}`,
    );
  }
};

const outputText = async (api: string, queue: string, jobId: string, name: string): Promise<string> => {
  const res = await fetch(`${api}/q/${queue}/j/${jobId}/outputs/${name}`, { redirect: "follow" });
  if (!res.ok) throw new Error(`GET outputs/${name} -> ${res.status}`);
  return await res.text();
};

// ---------------------------------------------------------------------------
// scenarios
// ---------------------------------------------------------------------------

const SCENARIOS: Scenario[] = [
  {
    name: "hello",
    description: "simplest possible container: run a command, write an output file",
    prompt: ({ api, queue }) =>
      `Run a container on the compute queue that prints ` +
      `"hello from the container" and writes the text "ok" to a file named result.txt in the ` +
      `outputs directory. Use the API at ${api} and the queue "${queue}". ` +
      `Confirm it exited 0 before you finish.`,
    verify: ({ api, queue, before }) =>
      verifyAnyJob(api, queue, before, async (jobId, result) => {
        assertOutputs(result, ["result.txt"]);
        const text = (await outputText(api, queue, jobId, "result.txt")).trim();
        if (text !== "ok") throw new Error(`result.txt should contain "ok", got: ${JSON.stringify(text)}`);
      }),
  },
  {
    name: "dockerfile",
    description: "author a Dockerfile that installs a dependency, then use it",
    prompt: ({ api, queue }) =>
      `Build a container that has Python 3 and the "jq" ` +
      `command-line tool available, then run it so that it writes a file named versions.json to ` +
      `the outputs directory containing a JSON object with two keys: "python" (the python ` +
      `version string) and "jq" (the jq version string). Use the API at ${api} and the queue ` +
      `"${queue}". Iterate until the container exits 0 and the output file is correct.`,
    verify: ({ api, queue, before }) =>
      verifyAnyJob(api, queue, before, async (jobId, result) => {
        assertOutputs(result, ["versions.json"]);
        const raw = await outputText(api, queue, jobId, "versions.json");
        let parsed: Record<string, unknown>;
        try {
          parsed = JSON.parse(raw);
        } catch {
          throw new Error(`versions.json is not valid JSON: ${raw.slice(0, 200)}`);
        }
        for (const key of ["python", "jq"]) {
          if (!parsed[key] || typeof parsed[key] !== "string") {
            throw new Error(`versions.json missing a string "${key}": ${raw.slice(0, 200)}`);
          }
        }
      }),
  },
  {
    name: "inputs",
    description: "supply an input file, process it in the container, read the output back",
    prompt: ({ api, queue, workdir }) =>
      `Run a container that reads the file ` +
      `${join(workdir, "numbers.txt")} (one integer per line), sums the numbers, and writes the ` +
      `sum as plain text to a file named sum.txt in the outputs directory. You must pass that ` +
      `local file into the container as an input. Use the API at ${api} and the queue "${queue}". ` +
      `Download the output and tell me the sum.`,
    verify: ({ api, queue, before }) =>
      verifyAnyJob(api, queue, before, async (jobId, result) => {
        assertOutputs(result, ["sum.txt"]);
        const text = (await outputText(api, queue, jobId, "sum.txt")).trim();
        // numbers.txt is 1..100 → 5050
        if (text !== "5050") throw new Error(`sum.txt should be 5050, got: ${JSON.stringify(text)}`);
      }),
  },
];

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

const setupWorkdir = async (scenario: Scenario): Promise<string> => {
  const workdir = await Deno.makeTempDir({ prefix: `skill-ai-${scenario.name}-` });
  if (scenario.name === "inputs") {
    const numbers = Array.from({ length: 100 }, (_, i) => i + 1).join("\n");
    await Deno.writeTextFile(join(workdir, "numbers.txt"), `${numbers}\n`);
  }
  return workdir;
};

/**
 * Install the skill into the session's own project-local skills directory, so
 * the run exercises the exact files in this repo and cannot be contaminated by
 * (or contaminate) a globally installed copy.
 */
const installSkill = async (workdir: string) => {
  const target = join(workdir, ".claude", "skills", "compute-queues");
  await Deno.mkdir(target, { recursive: true });
  for (const rel of ["SKILL.md", "references", "scripts"]) {
    await copyPath(join(SKILL_SRC, rel), join(target, rel));
  }
  return target;
};

const copyPath = async (src: string, dest: string) => {
  const info = await Deno.stat(src);
  if (info.isDirectory) {
    await Deno.mkdir(dest, { recursive: true });
    for await (const entry of Deno.readDir(src)) {
      await copyPath(join(src, entry.name), join(dest, entry.name));
    }
  } else {
    await Deno.copyFile(src, dest);
  }
};

const runClaude = async (
  prompt: string,
  workdir: string,
  timeoutMs: number,
  model?: string,
): Promise<{ code: number; output: string }> => {
  // cwd is the scenario's temp dir, which holds .claude/skills/compute-queues
  // and nothing else — so the session sees this repo's skill and no other
  // project context. bypassPermissions is required for an unattended run.
  const args = ["-p", prompt, "--permission-mode", "bypassPermissions", "--add-dir", workdir];
  if (model) args.push("--model", model);

  const command = new Deno.Command("claude", {
    args,
    cwd: workdir,
    stdout: "piped",
    stderr: "piped",
  });

  const child = command.spawn();
  const timer = setTimeout(() => {
    try {
      child.kill("SIGKILL");
    } catch { /* already gone */ }
  }, timeoutMs);

  const { code, stdout, stderr } = await child.output();
  clearTimeout(timer);
  const decoder = new TextDecoder();
  return { code, output: `${decoder.decode(stdout)}\n${decoder.decode(stderr)}` };
};

const main = async () => {
  const flags = parseArgs(Deno.args, {
    string: ["api", "queue", "scenario", "timeout", "model"],
    boolean: ["keep", "list"],
    default: {
      api: Deno.env.get("CQ_API") || "http://localhost:8000",
      timeout: "900",
    },
  });

  if (flags.list) {
    for (const s of SCENARIOS) console.log(`${s.name.padEnd(12)} ${s.description}`);
    return;
  }

  const api = String(flags.api).replace(/\/$/, "");
  const timeoutMs = Number(flags.timeout) * 1000;
  const selected = flags.scenario ? SCENARIOS.filter((s) => s.name === flags.scenario) : SCENARIOS;
  if (!selected.length) {
    console.error(`unknown scenario: ${flags.scenario}. Known: ${SCENARIOS.map((s) => s.name).join(", ")}`);
    Deno.exit(2);
  }

  // Preflight: the harness must not report a skill failure when the real
  // problem is that nothing could have run.
  try {
    await fetch(`${api}/healthz`);
  } catch (err) {
    console.error(`✗ cannot reach the API at ${api}: ${err instanceof Error ? err.message : err}`);
    console.error(`  start a local-mode worker first (it serves the API too):  just app/worker/local`);
    Deno.exit(2);
  }
  try {
    const cmd = await new Deno.Command("claude", { args: ["--version"], stdout: "null", stderr: "null" }).output();
    if (!cmd.success) throw new Error("claude --version failed");
  } catch {
    console.error("✗ `claude` is not on PATH — this harness drives a real Claude Code session");
    Deno.exit(2);
  }

  const results: { name: string; ok: boolean; detail: string; ms: number }[] = [];

  // A worker subscribes to exactly one queue, so the harness cannot invent a
  // fresh queue per scenario — it must use the queue the worker is on.
  // Isolation instead comes from snapshotting the queue before each run and
  // only considering jobs that appeared afterwards.
  const queue = String(flags.queue || Deno.env.get("CQ_QUEUE") || "local");

  const preflight = await queueStatus(api, queue).catch(() => undefined);
  const workerCount = Object.keys(preflight?.localWorkers || {}).length +
    Object.keys(preflight?.otherServers || {}).length;
  if (!workerCount) {
    console.warn(
      `⚠️  no workers visible on queue "${queue}". Jobs submitted there will sit in Queued ` +
        `forever. Pass --queue <the queue your worker is on> (the local-mode dev worker uses ` +
        `"local"; the remote dev stack uses "local1").`,
    );
  }

  for (const scenario of selected) {
    const workdir = await setupWorkdir(scenario);
    await installSkill(workdir);
    const before = new Set(await jobsOnQueue(api, queue));

    console.log(`\n▶ ${scenario.name}: ${scenario.description}`);
    console.log(`  queue   ${queue} (${before.size} pre-existing job(s) ignored)`);
    console.log(`  workdir ${workdir}`);

    const started = Date.now();
    const { code, output } = await runClaude(
      scenario.prompt({ api, queue, workdir }),
      workdir,
      timeoutMs,
      flags.model ? String(flags.model) : undefined,
    );
    const ms = Date.now() - started;

    let ok = false;
    let detail = "";
    try {
      await scenario.verify({ api, queue, workdir, before });
      ok = true;
      detail = "verified";
    } catch (err) {
      detail = err instanceof Error ? err.message : String(err);
    }

    if (!ok) {
      console.log(`  claude exited ${code}`);
      console.log(`  ---- session output (tail) ----`);
      console.log(output.split("\n").slice(-40).join("\n"));
      console.log(`  -------------------------------`);
    }
    console.log(`  ${ok ? "✓" : "✗"} ${detail} (${Math.round(ms / 1000)}s)`);
    results.push({ name: scenario.name, ok, detail, ms });

    if (!flags.keep) {
      await Deno.remove(workdir, { recursive: true }).catch(() => {});
    } else {
      console.log(`  kept ${workdir}`);
    }
  }

  console.log(`\n${"=".repeat(60)}`);
  for (const r of results) {
    console.log(`${r.ok ? "✓" : "✗"} ${r.name.padEnd(12)} ${Math.round(r.ms / 1000)}s  ${r.ok ? "" : r.detail}`);
  }
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} scenarios passed`);
  Deno.exit(failed.length ? 1 : 0);
};

if (import.meta.main) {
  await main();
}
