import { assert, assertEquals } from "std/assert";

import { DataRefType, type DockerJobDefinitionInputRefs, sha256Buffer } from "@metapages/compute-queues-shared";

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL")!;

type Submitted = { jobId: string; runId: string; jobHash: string; inputsHash?: string; path: string };

const submit = async (body: unknown): Promise<Submitted> => {
  const response = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  assertEquals(response.status, 200);
  return await response.json();
};

const waitForResult = async (url: string) => {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    const response = await fetch(url);
    if (response.ok) {
      const { data } = await response.json();
      if (data?.finished?.result) {
        return data.finished.result;
      }
    } else {
      await response.body?.cancel();
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`No result at ${url}`);
};

const logsText = (result: { logs: [string, number][] }) => result.logs.map((line) => line[0]).join("").trim();

const utf8 = (value: string) => ({ type: DataRefType.utf8, value });

Deno.test("One job processes many inputs, each addressed by /j/<job>?i=<inputs>", async () => {
  const nonce = crypto.randomUUID();
  const definition: DockerJobDefinitionInputRefs = {
    image: "alpine:3.18.5",
    command: `sh -c 'cat /inputs/data.txt; echo -n ${nonce} > /dev/null'`,
  };

  const a = await submit({ definition, inputs: { "data.txt": utf8("input-a") } });
  const b = await submit({ definition, inputs: { "data.txt": utf8("input-b") } });

  assertEquals(a.jobId, b.jobId);
  assertEquals(a.jobId, a.jobHash);
  assert(a.runId !== b.runId);
  assert(a.inputsHash !== b.inputsHash);
  assertEquals(a.path, `/j/${a.jobId}?i=${a.inputsHash}`);

  assertEquals(logsText(await waitForResult(`${API_URL}/q/${QUEUE_ID}/j/${a.runId}/result.json`)), "input-a");
  assertEquals(
    logsText(await waitForResult(`${API_URL}/j/${b.jobId}/result.json?i=${b.inputsHash}`)),
    "input-b",
  );

  // the job itself has no inputs, its runs do
  const job = await (await fetch(`${API_URL}/j/${a.jobId}/definition.json`)).json();
  assertEquals(job.data.command, definition.command);
  assertEquals(job.data.inputs, undefined);
  const run = await (await fetch(`${API_URL}/j/${a.jobId}/definition.json?i=${a.inputsHash}`)).json();
  assertEquals(run.data.inputs, { "data.txt": utf8("input-a") });

  // the job's id alone is enough to run it on something new
  const c = await submit({ jobId: a.jobId, inputs: { "data.txt": utf8("input-c") } });
  assertEquals(c.jobId, a.jobId);
  assertEquals(
    logsText(await waitForResult(`${API_URL}/q/${QUEUE_ID}${c.path.replace("?", "/result.json?")}`)),
    "input-c",
  );

  const missing = await fetch(`${API_URL}/j/${a.jobId}/result.json?i=${await sha256Buffer(new Uint8Array(1))}`);
  await missing.body?.cancel();
  assertEquals(missing.status, 404);
});

Deno.test("A job named with a uuid gets versions, addressed by /j/<uuid>?v=<version>", async () => {
  const jobId = crypto.randomUUID();
  const definitionFor = (text: string): DockerJobDefinitionInputRefs => ({
    image: "alpine:3.18.5",
    command: `echo ${text} ${jobId}`,
  });

  const v1 = await submit({ jobId, definition: definitionFor("one") });
  assertEquals(v1.jobId, jobId);
  assertEquals(v1.path, `/j/${jobId}?v=${v1.jobHash}`);
  await waitForResult(`${API_URL}/j/${jobId}/result.json`);

  const v2 = await submit({ jobId, definition: definitionFor("two") });
  assert(v1.jobHash !== v2.jobHash);

  assertEquals(logsText(await waitForResult(`${API_URL}/j/${jobId}/result.json`)), `two ${jobId}`);
  assertEquals(logsText(await waitForResult(`${API_URL}/j/${jobId}/result.json?v=${v1.jobHash}`)), `one ${jobId}`);
  assertEquals(
    logsText(await waitForResult(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/result.json?v=${v2.jobHash}`)),
    `two ${jobId}`,
  );
});

Deno.test("Job files are mounted relative to the working directory, or at their absolute path", async () => {
  const nonce = crypto.randomUUID();
  const { jobId } = await submit({
    definition: {
      image: "alpine:3.18.5",
      workdir: "/work",
      command: `sh -c 'pwd; sh run.sh; cat nested/dir/data.txt; cat /etc/job/config.txt'`,
      files: {
        "run.sh": utf8(`echo script ${nonce}`),
        "nested/dir/data.txt": utf8("nested"),
        "/etc/job/config.txt": utf8("absolute"),
      },
    },
  });
  const result = await waitForResult(`${API_URL}/j/${jobId}/result.json`);
  assertEquals(result.StatusCode, 0);
  assertEquals(logsText(result).split(/\s+/).join(" "), `/work script ${nonce} nestedabsolute`);

  const served = await fetch(`${API_URL}/j/${jobId}/files/run.sh`);
  assertEquals(await served.text(), `echo script ${nonce}`);
});

Deno.test("Job files land in the image's WORKDIR when the job does not set one", async () => {
  const nonce = crypto.randomUUID();
  const { jobId } = await submit({
    definition: {
      build: { dockerfile: "FROM alpine:3.18.5\nWORKDIR /from-image\n" },
      command: `sh -c 'pwd; cat data.txt'`,
      files: { "data.txt": utf8(nonce) },
    },
  });
  const result = await waitForResult(`${API_URL}/j/${jobId}/result.json`);
  assertEquals(logsText(result).split(/\s+/).join(" "), `/from-image ${nonce}`);
});

Deno.test("Definitions with `inputs` and `configFiles` still land in /inputs", async () => {
  const nonce = crypto.randomUUID();
  const { jobId, runId } = await submit({
    definition: {
      image: "alpine:3.18.5",
      command: `sh -c 'cat /inputs/in.txt /inputs/config.txt'`,
      inputs: { "in.txt": utf8(nonce) },
      configFiles: { "config.txt": utf8("config") },
    },
  });
  assertEquals(jobId, runId);
  const result = await waitForResult(`${API_URL}/j/${jobId}/result.json`);
  assertEquals(logsText(result), `${nonce}config`);
});

Deno.test("A file name cannot escape the job's folder on the worker", async () => {
  const { jobId } = await submit({
    definition: {
      image: "alpine:3.18.5",
      command: `echo ${crypto.randomUUID()}`,
      files: { "../../../../escaped.txt": utf8("nope") },
    },
  });
  const result = await waitForResult(`${API_URL}/j/${jobId}/result.json`);
  assert(`${result.error}`.includes("Invalid file name"));
});

const submitRaw = (body: unknown) =>
  fetch(`${API_URL}/q/${QUEUE_ID}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

const twoDefinitions = () => {
  const nonce = crypto.randomUUID();
  return {
    a: { image: "alpine:3.18.5", command: `sh -c 'echo A ${nonce}; cat /inputs/data.txt 2>/dev/null; true'` },
    b: { image: "alpine:3.18.5", command: `sh -c 'echo B ${nonce}; cat /inputs/data.txt 2>/dev/null; true'` },
    nonce,
  };
};

Deno.test("A sha256 jobId cannot be pointed at other content", async () => {
  const { a, b, nonce } = twoDefinitions();
  const jobA = await submit({ definition: a });
  await waitForResult(`${API_URL}/j/${jobA.jobId}/result.json`);

  const hijack = await submitRaw({ jobId: jobA.jobId, definition: b });
  assertEquals(hijack.status, 400);
  await hijack.body?.cancel();

  // the hash still runs its own content
  const run = await submit({ jobId: jobA.jobId, inputs: { "data.txt": utf8("x") } });
  assertEquals(run.jobHash, jobA.jobId);
  assertEquals(logsText(await waitForResult(`${API_URL}${run.path.replace("?", "/result.json?")}`)), `A ${nonce}\nx`);
  const definition = await (await fetch(`${API_URL}/j/${jobA.jobId}/definition.json`)).json();
  assertEquals(definition.data.command, a.command);
});

Deno.test("Hashes sent by a client are not believed", async () => {
  const { a, b, nonce } = twoDefinitions();
  const jobA = await submit({ definition: a });
  await waitForResult(`${API_URL}/j/${jobA.jobId}/result.json`);
  const forgedInputs = await sha256Buffer(crypto.getRandomValues(new Uint8Array(8)));

  for (
    const forged of [
      // a client that predates inputs, with its own id
      { id: crypto.randomUUID(), jobHash: jobA.jobId, inputsHash: forgedInputs, definition: b },
      // the ids derived on the server
      { jobHash: jobA.jobId, inputsHash: forgedInputs, definition: b },
      { jobId: crypto.randomUUID(), jobHash: jobA.jobId, inputsHash: forgedInputs, definition: b },
    ]
  ) {
    const response = await submitRaw(forged);
    assertEquals(response.status, 200);
    await response.body?.cancel();
    assertEquals(
      (await (await fetch(`${API_URL}/j/${jobA.jobId}/definition.json`)).json()).data.command,
      a.command,
    );
    const run = await fetch(`${API_URL}/j/${jobA.jobId}/result.json?i=${forgedInputs}`);
    await run.body?.cancel();
    assertEquals(run.status, 404);
  }
  assertEquals(logsText(await waitForResult(`${API_URL}/j/${jobA.jobId}/result.json`)), `A ${nonce}`);
});

Deno.test("A jobId has to be a sha256 of the job or a usable name", async () => {
  const { a } = twoDefinitions();
  for (const jobId of ["has/slash", "has space", "a?b", "a#b", "foo.json", "x".repeat(129), "A".repeat(64)]) {
    const response = await submitRaw({ jobId, definition: a });
    assertEquals(response.status, 400, jobId);
    assert((await response.json()).error.includes("Invalid jobId"));
  }
});

Deno.test("POST routes work through a job's name", async () => {
  const jobId = crypto.randomUUID();
  const { runId } = await submit({
    jobId,
    definition: { image: "alpine:3.18.5", command: `sh -c 'sleep 60; echo ${jobId}'` },
  });
  const cancelled = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/cancel`, { method: "POST" });
  assertEquals(cancelled.status, 200);
  assertEquals((await cancelled.json()).success, true);

  const deadline = Date.now() + 30_000;
  while (true) {
    const { data } = await (await fetch(`${API_URL}/q/${QUEUE_ID}/j`)).json();
    if (!data?.[runId] || data[runId].state === "Finished") {
      break;
    }
    assert(Date.now() < deadline, "the job was not cancelled");
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  // There is one queue in local mode: the local handler refuses every copy
  if (QUEUE_ID !== "local") {
    const copyName = crypto.randomUUID();
    const inputs = { "data.txt": utf8(copyName) };
    const run = await submit({ jobId: copyName, definition: twoDefinitions().a, inputs });
    await waitForResult(`${API_URL}/j/${run.runId}/result.json`);
    const otherQueue = `copied-${copyName}`;
    const copied = await fetch(`${API_URL}/j/${copyName}/copy?i=${run.inputsHash}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ queue: otherQueue }),
    });
    assertEquals(copied.status, 200);
    await copied.body?.cancel();
    const onOtherQueue = await (await fetch(`${API_URL}/q/${otherQueue}/j/${run.runId}`)).json();
    assert(onOtherQueue.data);
  }
});
