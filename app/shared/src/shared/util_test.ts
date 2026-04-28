import { assertEquals, assertNotEquals } from "std/assert";

import { DataRefType, type DockerJobDefinitionInputRefs } from "@shared/types.ts";
import { shaDockerJob } from "@shared/util.ts";

// A minimal job definition used as a base for tests
const baseJob: DockerJobDefinitionInputRefs = {
  image: "alpine:latest",
  command: "echo hello",
};

Deno.test("shaDockerJob: same job produces same hash", async () => {
  const hash1 = await shaDockerJob({ ...baseJob });
  const hash2 = await shaDockerJob({ ...baseJob });
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: extra unknown fields do not change the hash", async () => {
  const hashClean = await shaDockerJob({ ...baseJob });

  // Add fields that don't exist in the type definition
  const jobWithExtras = {
    ...baseJob,
    someRandomField: "should be ignored",
    _internal: { foo: "bar" },
    timestamp: Date.now(),
  } as DockerJobDefinitionInputRefs;

  const hashWithExtras = await shaDockerJob(jobWithExtras);
  assertEquals(hashClean, hashWithExtras);
});

Deno.test("shaDockerJob: channel and CHANNEL env vars are excluded", async () => {
  const jobNoChannel: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: { FOO: "bar" },
  };
  const jobWithChannel: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: { FOO: "bar", channel: "abc123" },
  };
  const jobWithCHANNEL: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: { FOO: "bar", CHANNEL: "xyz789" },
  };

  const hash1 = await shaDockerJob(jobNoChannel);
  const hash2 = await shaDockerJob(jobWithChannel);
  const hash3 = await shaDockerJob(jobWithCHANNEL);
  assertEquals(hash1, hash2);
  assertEquals(hash1, hash3);
});

Deno.test("shaDockerJob: different commands produce different hashes", async () => {
  const hash1 = await shaDockerJob({ ...baseJob, command: "echo hello" });
  const hash2 = await shaDockerJob({ ...baseJob, command: "echo world" });
  assertNotEquals(hash1, hash2);
});

Deno.test("shaDockerJob: presigned URL params are stripped from inputs", async () => {
  const jobWithPresigned: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value: "https://example.com/data/presignedurl/abc123?token=xyz",
        type: DataRefType.url,
      },
    },
  };
  const jobWithoutPresigned: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value: "https://example.com/data",
        type: DataRefType.url,
      },
    },
  };

  const hash1 = await shaDockerJob(jobWithPresigned);
  const hash2 = await shaDockerJob(jobWithoutPresigned);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: presigned URL params are stripped from configFiles", async () => {
  const jobWithPresigned: DockerJobDefinitionInputRefs = {
    ...baseJob,
    configFiles: {
      config: {
        value: "https://example.com/config/presignedurl/abc123?token=xyz",
        type: DataRefType.url,
      },
    },
  };
  const jobWithoutPresigned: DockerJobDefinitionInputRefs = {
    ...baseJob,
    configFiles: {
      config: {
        value: "https://example.com/config",
        type: DataRefType.url,
      },
    },
  };

  const hash1 = await shaDockerJob(jobWithPresigned);
  const hash2 = await shaDockerJob(jobWithoutPresigned);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: S3 query params are stripped from URLs", async () => {
  const jobWithParams: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value:
          "https://metaframe-asman-test.s3.us-west-1.amazonaws.com/data/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=abc",
        type: DataRefType.url,
      },
    },
  };
  const jobWithoutParams: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value: "https://metaframe-asman-test.s3.us-west-1.amazonaws.com/data/file.txt",
        type: DataRefType.url,
      },
    },
  };

  const hash1 = await shaDockerJob(jobWithParams);
  const hash2 = await shaDockerJob(jobWithoutParams);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: DataRef hash field is excluded", async () => {
  const jobWithHash: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value: "some data",
        type: DataRefType.utf8,
        hash: "abc123deadbeef",
      },
    },
  };
  const jobWithoutHash: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: {
        value: "some data",
        type: DataRefType.utf8,
      },
    },
  };

  const hash1 = await shaDockerJob(jobWithHash);
  const hash2 = await shaDockerJob(jobWithoutHash);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: extra fields on build sub-object are ignored", async () => {
  const jobClean: DockerJobDefinitionInputRefs = {
    ...baseJob,
    build: {
      context: "https://github.com/example/repo.git",
      dockerfile: "FROM alpine\nRUN echo hi",
    },
  };
  const jobWithExtras: DockerJobDefinitionInputRefs = {
    ...baseJob,
    build: {
      context: "https://github.com/example/repo.git",
      dockerfile: "FROM alpine\nRUN echo hi",
      // deno-lint-ignore no-explicit-any
      ...({ someFutureField: "should be ignored" } as any),
    },
  };

  const hash1 = await shaDockerJob(jobClean);
  const hash2 = await shaDockerJob(jobWithExtras);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: extra fields on requirements sub-object are ignored", async () => {
  const jobClean: DockerJobDefinitionInputRefs = {
    ...baseJob,
    requirements: { cpus: 2, gpus: 1 },
  };
  const jobWithExtras: DockerJobDefinitionInputRefs = {
    ...baseJob,
    requirements: {
      cpus: 2,
      gpus: 1,
      // deno-lint-ignore no-explicit-any
      ...({ someFutureField: "should be ignored" } as any),
    },
  };

  const hash1 = await shaDockerJob(jobClean);
  const hash2 = await shaDockerJob(jobWithExtras);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: all known scalar fields affect the hash", async () => {
  const hashes = new Set<string>();

  const variants: DockerJobDefinitionInputRefs[] = [
    { ...baseJob },
    { ...baseJob, v: 2 },
    { ...baseJob, entrypoint: "/bin/sh" },
    { ...baseJob, workdir: "/app" },
    { ...baseJob, shmSize: "256m" },
    { ...baseJob, maxDuration: "1h" },
    { ...baseJob, tags: ["gpu"] },
  ];

  for (const variant of variants) {
    hashes.add(await shaDockerJob(variant));
  }

  // Each variant should produce a unique hash
  assertEquals(hashes.size, variants.length);
});

Deno.test("shaDockerJob: full job with all fields produces stable hash", async () => {
  const fullJob: DockerJobDefinitionInputRefs = {
    v: 1,
    image: "python:3.11",
    command: "python main.py",
    entrypoint: "/bin/sh",
    workdir: "/workspace",
    shmSize: "512m",
    maxDuration: "30m",
    tags: ["gpu", "large"],
    build: {
      context: "https://github.com/example/repo.git",
      filename: "Dockerfile",
      target: "runtime",
      buildArgs: ["--no-cache"],
      platform: "linux/amd64",
    },
    requirements: { cpus: 4, gpus: 1, maxDuration: "1h", memory: "8g" },
    env: { PATH: "/usr/bin", MY_VAR: "value", channel: "ignored", CHANNEL: "also-ignored" },
    inputs: {
      data: { value: "aGVsbG8=", type: DataRefType.base64 },
    },
    configFiles: {
      "config.yaml": { value: "key: value", type: DataRefType.utf8 },
    },
  };

  const hash1 = await shaDockerJob(fullJob);
  const hash2 = await shaDockerJob(fullJob);
  assertEquals(hash1, hash2);

  // Same job with extra unknown fields should match
  const fullJobWithExtras = {
    ...fullJob,
    _addedByServer: true,
    metadata: { source: "api" },
  } as DockerJobDefinitionInputRefs;
  const hash3 = await shaDockerJob(fullJobWithExtras);
  assertEquals(hash1, hash3);
});

Deno.test("shaDockerJob: env with only channel vars produces same hash as no env", async () => {
  const jobNoEnv: DockerJobDefinitionInputRefs = { ...baseJob };
  const jobOnlyChannel: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: { channel: "abc", CHANNEL: "xyz" },
  };

  const hash1 = await shaDockerJob(jobNoEnv);
  const hash2 = await shaDockerJob(jobOnlyChannel);
  assertEquals(hash1, hash2);
});
