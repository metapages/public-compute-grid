import { assertEquals, assertExists } from "std/assert";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { listMCPTools, mcpListJobs, mcpSubmitJob, QUEUE_ID, waitForJobCompletion } from "./mcp_util.ts";

Deno.test("MCP Integration: list available tools", async () => {
  const tools = await listMCPTools();

  console.log("Available MCP tools:");
  tools.forEach((tool) => {
    console.log(`  - ${tool.name}: ${tool.description}`);
  });

  assertExists(tools);
  assertEquals(tools.length > 0, true, "Should have at least one tool");

  // Verify expected tools are present
  const toolNames = tools.map((t) => t.name);
  assertEquals(toolNames.includes("submit_job"), true);
  assertEquals(toolNames.includes("get_job_status"), true);
  assertEquals(toolNames.includes("list_jobs"), true);
  assertEquals(toolNames.includes("cancel_job"), true);

  closeKv();
});

Deno.test("MCP Integration: complete workflow - build and run a simple app", async () => {
  console.log("\n=== Starting MCP Integration Test ===\n");

  // Step 1: Submit a job that builds and runs a simple Python script
  console.log("Step 1: Submitting job with inline code...");

  const pythonCode = `
print("Hello from Python!")
print("Testing MCP integration")
import sys
print(f"Python version: {sys.version}")
`;

  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "python:3.11-alpine",
    command: "python /inputs/script.py",
    inputs: {
      "script.py": pythonCode,
    },
    namespace: "mcp-integration",
    maxDuration: "5m",
  });

  console.log(`Job submitted successfully! Job ID: ${result.jobId}`);
  assertExists(result.jobId);
  assertEquals(result.success, true);

  // Step 2: Wait for job completion and monitor progress
  console.log("\nStep 2: Monitoring job execution...");

  const finalStatus = await waitForJobCompletion(
    QUEUE_ID,
    result.jobId,
    120000,
    2000,
  );

  console.log("\nJob completed!");
  console.log(`Final state: ${finalStatus.status?.state}`);
  console.log(`Exit code: ${finalStatus.result?.statusCode}`);

  // Step 3: Verify the results
  console.log("\nStep 3: Verifying results...");

  assertEquals(finalStatus.status?.state, "Finished");
  assertEquals(finalStatus.result?.statusCode, 0);

  // Check logs contain expected output
  const logs = finalStatus.result?.logs?.map((l) => l[0]).join("") || "";
  console.log("\nJob output:");
  console.log(logs);

  assertEquals(logs.includes("Hello from Python!"), true);
  assertEquals(logs.includes("Testing MCP integration"), true);
  assertEquals(logs.includes("Python version"), true);

  console.log("\n=== Integration Test Complete ===\n");

  closeKv();
});

Deno.test("MCP Integration: build from git repository", async () => {
  console.log("\n=== Testing Git Repository Build ===\n");

  // This is a simple example repository
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    gitRepo: "https://github.com/octocat/Hello-World.git",
    command: "ls -la && echo 'Repository cloned successfully'",
    namespace: "mcp-integration-git",
    maxDuration: "10m",
  });

  console.log(`Git job submitted! Job ID: ${result.jobId}`);

  const finalStatus = await waitForJobCompletion(
    QUEUE_ID,
    result.jobId,
    180000, // 3 minutes for git clone
  );

  console.log(`Job state: ${finalStatus.status?.state}`);
  assertEquals(finalStatus.status?.state, "Finished");

  const logs = finalStatus.result?.logs?.map((l) => l[0]).join("") || "";
  console.log("\nRepository contents:");
  console.log(logs);

  assertEquals(logs.includes("Repository cloned successfully"), true);

  closeKv();
});

Deno.test("MCP Integration: multi-step workflow with environment variables", async () => {
  console.log("\n=== Testing Multi-Step Workflow ===\n");

  const shellScript = `
#!/bin/sh
echo "Step 1: Reading configuration"
echo "Environment: $ENV_NAME"
echo "Version: $APP_VERSION"

echo "Step 2: Processing input file"
cat /inputs/config.txt

echo "Step 3: Generating output"
echo "Processed at: $(date)" > /outputs/result.txt
echo "Environment: $ENV_NAME" >> /outputs/result.txt

echo "Step 4: Complete"
cat /outputs/result.txt
`;

  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sh /inputs/script.sh",
    inputs: {
      "script.sh": shellScript,
      "config.txt": "Configuration data for processing",
    },
    env: {
      ENV_NAME: "mcp-test",
      APP_VERSION: "1.0.0",
    },
    namespace: "mcp-integration-multi",
  });

  console.log(`Multi-step job submitted! Job ID: ${result.jobId}`);

  const finalStatus = await waitForJobCompletion(QUEUE_ID, result.jobId, 60000);

  assertEquals(finalStatus.status?.state, "Finished");
  assertEquals(finalStatus.result?.statusCode, 0);

  const logs = finalStatus.result?.logs?.map((l) => l[0]).join("") || "";
  console.log("\nWorkflow output:");
  console.log(logs);

  assertEquals(logs.includes("Step 1: Reading configuration"), true);
  assertEquals(logs.includes("Environment: mcp-test"), true);
  assertEquals(logs.includes("Version: 1.0.0"), true);
  assertEquals(logs.includes("Configuration data for processing"), true);
  assertEquals(logs.includes("Step 4: Complete"), true);

  console.log("\n=== Multi-Step Workflow Complete ===\n");

  closeKv();
});

Deno.test("MCP Integration: test concurrent job submission", async () => {
  console.log("\n=== Testing Concurrent Job Submission ===\n");

  // Submit multiple jobs concurrently
  const jobPromises = [];

  for (let i = 0; i < 3; i++) {
    const promise = mcpSubmitJob({
      queue: QUEUE_ID,
      image: "alpine:3.18.5",
      command: `echo "Job ${i + 1}" && sleep ${i + 1}`,
      namespace: "mcp-integration-concurrent",
    });
    jobPromises.push(promise);
  }

  const results = await Promise.all(jobPromises);

  console.log("All jobs submitted:");
  results.forEach((r, i) => {
    console.log(`  Job ${i + 1}: ${r.jobId}`);
  });

  assertEquals(results.length, 3);
  results.forEach((r) => {
    assertExists(r.jobId);
    assertEquals(r.success, true);
  });

  // List jobs to see them all
  const jobsList = await mcpListJobs({ queue: QUEUE_ID });
  console.log(`\nTotal jobs in queue: ${jobsList.totalJobs}`);

  // Wait for all to complete
  console.log("\nWaiting for all jobs to complete...");
  const completionPromises = results.map((r) => waitForJobCompletion(QUEUE_ID, r.jobId, 60000));

  const finalStatuses = await Promise.all(completionPromises);

  console.log("\nAll jobs completed:");
  finalStatuses.forEach((status, i) => {
    console.log(`  Job ${i + 1}: ${status.status?.state} (exit code: ${status.result?.statusCode})`);
    assertEquals(status.status?.state, "Finished");
    assertEquals(status.result?.statusCode, 0);
  });

  console.log("\n=== Concurrent Job Test Complete ===\n");

  closeKv();
});

Deno.test("MCP Integration: iterative container development simulation", async () => {
  console.log("\n=== Simulating Iterative Container Development ===\n");

  // Iteration 1: Try a simple script (that fails)
  console.log("Iteration 1: First attempt (will fail)...");

  const attempt1 = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "python script.py", // Will fail - no python installed
    namespace: "mcp-iteration",
  });

  const result1 = await waitForJobCompletion(QUEUE_ID, attempt1.jobId, 60000);
  console.log(`Result: ${result1.finishedReason} (exit code: ${result1.result?.statusCode})`);

  assertEquals(result1.result?.statusCode !== 0, true, "First attempt should fail");

  // Iteration 2: Fix by using Python image
  console.log("\nIteration 2: Using correct base image...");

  const attempt2 = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "python:3.11-alpine",
    command: "python /inputs/script.py",
    inputs: {
      "script.py": "print('Hello')\nimport missing_module", // Will fail - missing module
    },
    namespace: "mcp-iteration",
  });

  const result2 = await waitForJobCompletion(QUEUE_ID, attempt2.jobId, 60000);
  console.log(`Result: ${result2.finishedReason} (exit code: ${result2.result?.statusCode})`);

  assertEquals(result2.result?.statusCode !== 0, true, "Second attempt should fail");

  // Iteration 3: Fix the script
  console.log("\nIteration 3: Fixed script...");

  const attempt3 = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "python:3.11-alpine",
    command: "python /inputs/script.py",
    inputs: {
      "script.py": "print('Success! Container is working correctly')",
    },
    namespace: "mcp-iteration",
  });

  const result3 = await waitForJobCompletion(QUEUE_ID, attempt3.jobId, 60000);
  console.log(`Result: ${result3.finishedReason} (exit code: ${result3.result?.statusCode})`);

  assertEquals(result3.status?.state, "Finished");
  assertEquals(result3.result?.statusCode, 0);

  const logs = result3.result?.logs?.map((l) => l[0]).join("") || "";
  assertEquals(logs.includes("Success! Container is working correctly"), true);

  console.log("\n=== Iterative Development Simulation Complete ===");
  console.log("This demonstrates how an LLM can iteratively fix containers using MCP!\n");

  closeKv();
});
