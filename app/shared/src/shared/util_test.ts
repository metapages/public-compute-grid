import { assertEquals, assertNotEquals } from "std/assert";

import { DataRefType, type DockerJobDefinitionInputRefs } from "@shared/types.ts";
import { isParentInjectedHashParamKey, shaDockerJob } from "@shared/util.ts";

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

Deno.test("shaDockerJob: metapage-injected env keys do not affect the hash", async () => {
  const jobNoEnv: DockerJobDefinitionInputRefs = { ...baseJob };

  // metapage.io injects a fresh mp-channel uuid on every page load for anonymous
  // viewers; two refreshes of the same metapage must produce the same job id.
  const refresh1: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: {
      "mp-channel": "wss://router.metapage.io/8bb1e0f24a5a4d0f9c6b1e2d3f4a5b6c",
      "parent-metapage-id": "7ca89718c03b4b97b06e2312dce2cc11",
      "fs-path": "/some/path",
    },
  };
  const refresh2: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: {
      "mp-channel": "wss://router.metapage.io/1111222233334444555566667777888",
      "parent-metapage-id": "7ca89718c03b4b97b06e2312dce2cc11",
      "fs-path": "/some/path",
    },
  };

  const hashNoEnv = await shaDockerJob(jobNoEnv);
  assertEquals(await shaDockerJob(refresh1), hashNoEnv);
  assertEquals(await shaDockerJob(refresh2), hashNoEnv);
});

Deno.test("shaDockerJob: readonly/edit do not make job ids differ between viewers", async () => {
  // `readonly` is injected only when the viewer does not own the metaframe, so
  // the owner and a visitor must still compute the same job id.
  const owner: DockerJobDefinitionInputRefs = { ...baseJob };
  const visitor: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: { readonly: "true", edit: "true" },
  };

  assertEquals(await shaDockerJob(visitor), await shaDockerJob(owner));
});

Deno.test("shaDockerJob: any future mp- prefixed param is excluded automatically", async () => {
  const jobNoEnv: DockerJobDefinitionInputRefs = { ...baseJob };

  // The point of the reserved prefix: metapage.io can add new injected params
  // without this repo changing. Keys below do not exist today and must still
  // be excluded.
  const withFutureParams: DockerJobDefinitionInputRefs = {
    ...baseJob,
    env: {
      "mp-some-future-param": "abc",
      "mp_another_future_param": "def",
    },
  };

  assertEquals(await shaDockerJob(withFutureParams), await shaDockerJob(jobNoEnv));
});

Deno.test("isParentInjectedHashParamKey: classifies keys correctly", () => {
  // reserved prefix, both spellings
  assertEquals(isParentInjectedHashParamKey("mp-channel"), true);
  assertEquals(isParentInjectedHashParamKey("mp_debug"), true);
  assertEquals(isParentInjectedHashParamKey("mp-anything-new"), true);

  // frozen legacy set
  for (const key of ["channel", "CHANNEL", "readonly", "edit", "parent-metapage-id", "fs-path"]) {
    assertEquals(isParentInjectedHashParamKey(key), true, `expected ${key} to be parent-injected`);
  }

  // user env vars and container-owned params must pass through
  for (const key of ["FOO", "mpower", "MP_USER_VAR", "map-tiles", "image", "command"]) {
    assertEquals(isParentInjectedHashParamKey(key), false, `expected ${key} NOT to be parent-injected`);
  }
});

Deno.test("shaDockerJob: user env vars still affect the hash", async () => {
  const jobA: DockerJobDefinitionInputRefs = { ...baseJob, env: { FOO: "a" } };
  const jobB: DockerJobDefinitionInputRefs = { ...baseJob, env: { FOO: "b" } };

  const hashA = await shaDockerJob(jobA);
  const hashB = await shaDockerJob(jobB);
  assertNotEquals(hashA, hashB);
});

Deno.test("shaDockerJob: null scalar fields produce same hash as omitted fields", async () => {
  const jobClean: DockerJobDefinitionInputRefs = { ...baseJob };

  // Simulate an external client sending explicit nulls for optional fields
  const jobWithNulls = {
    ...baseJob,
    v: null,
    entrypoint: null,
    workdir: null,
    shmSize: null,
    maxDuration: null,
    tags: null,
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobClean);
  const hash2 = await shaDockerJob(jobWithNulls);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: null fields in build sub-object produce same hash as omitted fields", async () => {
  const jobWithBuild: DockerJobDefinitionInputRefs = {
    ...baseJob,
    build: {
      context: "https://github.com/example/repo.git",
      dockerfile: "FROM alpine\nRUN echo hi",
    },
  };
  const jobWithNullBuildFields = {
    ...baseJob,
    build: {
      context: "https://github.com/example/repo.git",
      dockerfile: "FROM alpine\nRUN echo hi",
      filename: null,
      target: null,
      buildArgs: null,
      platform: null,
    },
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobWithBuild);
  const hash2 = await shaDockerJob(jobWithNullBuildFields);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: null fields in requirements sub-object produce same hash as omitted fields", async () => {
  const jobWithReqs: DockerJobDefinitionInputRefs = {
    ...baseJob,
    requirements: { cpus: 2 },
  };
  const jobWithNullReqFields = {
    ...baseJob,
    requirements: {
      cpus: 2,
      gpus: null,
      maxDuration: null,
      memory: null,
    },
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobWithReqs);
  const hash2 = await shaDockerJob(jobWithNullReqFields);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: empty build object produces same hash as no build", async () => {
  const jobNoBuild: DockerJobDefinitionInputRefs = { ...baseJob };
  const jobEmptyBuild: DockerJobDefinitionInputRefs = {
    ...baseJob,
    // deno-lint-ignore no-explicit-any
    build: {} as any,
  };

  const hash1 = await shaDockerJob(jobNoBuild);
  const hash2 = await shaDockerJob(jobEmptyBuild);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: empty requirements object produces same hash as no requirements", async () => {
  const jobNoReqs: DockerJobDefinitionInputRefs = { ...baseJob };
  const jobEmptyReqs: DockerJobDefinitionInputRefs = {
    ...baseJob,
    // deno-lint-ignore no-explicit-any
    requirements: {} as any,
  };

  const hash1 = await shaDockerJob(jobNoReqs);
  const hash2 = await shaDockerJob(jobEmptyReqs);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: build with all-null fields produces same hash as no build", async () => {
  const jobNoBuild: DockerJobDefinitionInputRefs = { ...baseJob };
  const jobAllNullBuild = {
    ...baseJob,
    build: {
      context: null,
      buildContext: null,
      filename: null,
      target: null,
      dockerfile: null,
      buildArgs: null,
      platform: null,
    },
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobNoBuild);
  const hash2 = await shaDockerJob(jobAllNullBuild);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: requirements with all-null fields produces same hash as no requirements", async () => {
  const jobNoReqs: DockerJobDefinitionInputRefs = { ...baseJob };
  const jobAllNullReqs = {
    ...baseJob,
    requirements: {
      cpus: null,
      gpus: null,
      maxDuration: null,
      memory: null,
    },
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobNoReqs);
  const hash2 = await shaDockerJob(jobAllNullReqs);
  assertEquals(hash1, hash2);
});

Deno.test("shaDockerJob: input ref with null type produces same hash as omitted type", async () => {
  const jobWithType: DockerJobDefinitionInputRefs = {
    ...baseJob,
    inputs: {
      file1: { value: "some data" },
    },
  };
  const jobWithNullType = {
    ...baseJob,
    inputs: {
      file1: { value: "some data", type: null },
    },
  } as unknown as DockerJobDefinitionInputRefs;

  const hash1 = await shaDockerJob(jobWithType);
  const hash2 = await shaDockerJob(jobWithNullType);
  assertEquals(hash1, hash2);
});
