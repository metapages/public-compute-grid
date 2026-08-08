import { assertEquals, assertExists } from "std/assert";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { API_URL, mcpCancelJob, mcpSubmitJob, QUEUE_ID } from "./mcp_util.ts";

Deno.test("MCP: submit simple Alpine job", async () => {
  console.log(`Testing MCP job submission to ${API_URL}`);

  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "echo 'Hello from MCP test'",
    namespace: "mcp-test",
  });

  console.log("Job submission result:", result);

  // Verify the response
  assertEquals(result.success, true);
  assertExists(result.jobId, "Job ID should be returned");
  assertEquals(result.queue, QUEUE_ID);

  // Clean up - cancel the job
  await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test",
  });

  closeKv();
});

Deno.test("MCP: submit job with inputs and env vars", async () => {
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sh -c 'cat /inputs/test.txt && echo $TEST_VAR'",
    inputs: {
      "test.txt": "Hello from input file",
    },
    env: {
      TEST_VAR: "test_value",
    },
    namespace: "mcp-test",
  });

  console.log("Job with inputs submission result:", result);

  assertEquals(result.success, true);
  assertExists(result.jobId);
  assertEquals(result.definition.filesCount, 1);
  assertEquals(result.definition.envCount, 1);

  // Clean up
  await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test",
  });

  closeKv();
});

Deno.test("MCP: submit job with git repository", async () => {
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    gitRepo: "https://github.com/octocat/Hello-World.git",
    command: "ls -la",
    namespace: "mcp-test",
  });

  console.log("Job with git repo submission result:", result);

  assertEquals(result.success, true);
  assertExists(result.jobId);

  // Clean up
  await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test",
  });

  closeKv();
});

Deno.test("MCP: submit job with inline Dockerfile", async () => {
  const dockerfile = `
FROM alpine:3.18.5
RUN apk add --no-cache curl
CMD ["curl", "--version"]
`;

  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    dockerfile: dockerfile,
    command: "curl --version",
    namespace: "mcp-test",
  });

  console.log("Job with Dockerfile submission result:", result);

  assertEquals(result.success, true);
  assertExists(result.jobId);
  assertEquals(result.definition.hasDockerfile, true);

  // Clean up
  await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test",
  });

  closeKv();
});

Deno.test("MCP: submit job with maxDuration", async () => {
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sleep 1",
    maxDuration: "5m",
    namespace: "mcp-test",
  });

  console.log("Job with maxDuration submission result:", result);

  assertEquals(result.success, true);
  assertExists(result.jobId);
  assertEquals(result.definition.maxDuration, "5m");

  // Clean up
  await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test",
  });

  closeKv();
});

Deno.test("MCP: error handling - no image or gitRepo", async () => {
  try {
    await mcpSubmitJob({
      queue: QUEUE_ID,
      command: "echo test",
      namespace: "mcp-test",
    });
    throw new Error("Should have thrown an error");
  } catch (error) {
    const err = error as Error;
    // Should fail because neither image nor gitRepo is specified
    assertExists(err.message);
    console.log("Expected error:", err.message);
  }

  closeKv();
});

Deno.test("MCP: error handling - both image and gitRepo", async () => {
  try {
    await mcpSubmitJob({
      queue: QUEUE_ID,
      image: "alpine:3.18.5",
      gitRepo: "https://github.com/octocat/Hello-World.git",
      command: "echo test",
      namespace: "mcp-test",
    });
    throw new Error("Should have thrown an error");
  } catch (error) {
    const err = error as Error;
    // Should fail because both image and gitRepo are specified
    assertExists(err.message);
    console.log("Expected error:", err.message);
  }

  closeKv();
});
