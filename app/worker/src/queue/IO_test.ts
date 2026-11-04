import { assertEquals } from "std/assert";
import { join } from "std/path";

import { config } from "/@/config.ts";
import { convertIOToVolumeMounts } from "/@/queue/IO.ts";

const withOutputFromAnEarlierAttempt = async (check: (outputFile: string) => Promise<void>) => {
  const dataDirectory = config.dataDirectory;
  config.dataDirectory = await Deno.makeTempDir();
  try {
    const outputs = join(config.dataDirectory, "j", "job", "outputs");
    await Deno.mkdir(outputs, { recursive: true });
    await check(join(outputs, "attempt1.txt"));
  } finally {
    await Deno.remove(config.dataDirectory, { recursive: true });
    config.dataDirectory = dataDirectory;
  }
};

const job = { id: "job", definition: { image: "alpine:3.18.5" } };

Deno.test("a job starting over does not inherit the outputs of its killed attempt", async () => {
  await withOutputFromAnEarlierAttempt(async (outputFile) => {
    await Deno.writeTextFile(outputFile, "half written");
    await convertIOToVolumeMounts(job, "http://localhost", { keepOutputs: false });
    assertEquals([...Deno.readDirSync(join(outputFile, ".."))], []);
  });
});

Deno.test("emptying a job's outputs leaves the results cached from them", async () => {
  await withOutputFromAnEarlierAttempt(async (outputFile) => {
    const cached = join(config.dataDirectory, "f", "abc");
    await Deno.mkdir(join(config.dataDirectory, "f"));
    await Deno.writeTextFile(outputFile, "result");
    await Deno.link(outputFile, cached);
    await convertIOToVolumeMounts(job, "http://localhost", { keepOutputs: false });
    assertEquals(await Deno.readTextFile(cached), "result");
  });
});

Deno.test("reattaching to a running container keeps what it has written", async () => {
  await withOutputFromAnEarlierAttempt(async (outputFile) => {
    await Deno.writeTextFile(outputFile, "still being written");
    await convertIOToVolumeMounts(job, "http://localhost", { keepOutputs: true });
    assertEquals(await Deno.readTextFile(outputFile), "still being written");
  });
});
