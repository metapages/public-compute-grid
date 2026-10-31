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
