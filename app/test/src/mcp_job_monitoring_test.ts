import { assertEquals, assertExists } from "std/assert";
import { closeKv } from "../../shared/src/shared/kv.ts";
import {
  mcpCancelJob,
  mcpGetJobStatus,
  mcpListJobs,
  mcpSubmitJob,
  QUEUE_ID,
  waitForJobCompletion,
} from "./mcp_util.ts";

Deno.test("MCP: monitor job status from queued to finished", async () => {
  // Submit a simple job
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "echo 'Test complete' && sleep 1",
    namespace: "mcp-test",
  });

  console.log("Job submitted:", result.jobId);

  // Check initial status
  const initialStatus = await mcpGetJobStatus({
    queue: QUEUE_ID,
    jobId: result.jobId,
  });

  console.log("Initial job status:", initialStatus);

  assertExists(initialStatus.jobId);
  assertEquals(initialStatus.jobId, result.jobId);
  // State should be Queued or Running
  assertExists(initialStatus.status?.state);

  // Wait for job to complete
  console.log("Waiting for job completion...");
  const finalStatus = await waitForJobCompletion(QUEUE_ID, result.jobId, 60000);

  console.log("Final job status:", finalStatus);

  assertEquals(finalStatus.status?.state, "Finished");
  assertEquals(finalStatus.jobId, result.jobId);
  assertExists(finalStatus.result);

  closeKv();
});

Deno.test("MCP: get detailed job result with logs", async () => {
  // Submit a job that produces specific output
  const testMessage = "MCP monitoring test output";
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: `echo '${testMessage}'`,
    namespace: "mcp-test",
  });

  console.log("Job submitted:", result.jobId);

  // Wait for completion
  const finalStatus = await waitForJobCompletion(QUEUE_ID, result.jobId, 60000);

  console.log("Final status:", JSON.stringify(finalStatus, null, 2));

  // Verify we have logs
  assertExists(finalStatus.result);
  assertExists(finalStatus.result.logs);

  // Check if logs contain our message
  const logsText = finalStatus.result.logs.map((l) => l[0]).join("");
  console.log("Job logs:", logsText);

  assertEquals(finalStatus.result.statusCode, 0, "Job should complete successfully");

  closeKv();
});

Deno.test("MCP: list jobs in queue", async () => {
  // Submit a few jobs
  const job1 = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sleep 10",
    namespace: "mcp-test-list",
  });

  const job2 = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sleep 10",
    namespace: "mcp-test-list",
  });

  console.log("Jobs submitted:", job1.jobId, job2.jobId);

  // List all jobs
  const jobsList = await mcpListJobs({
    queue: QUEUE_ID,
  });

  console.log("Jobs in queue:", jobsList);

  assertExists(jobsList.jobs);
  assertExists(jobsList.totalJobs);

  // `jobs` is an array of {jobId, state, ...}, per app/mcp/src/tools/list-jobs.ts
  const ourJobs = (jobsList.jobs as Array<{ jobId: string }>).filter(
    (job) => job.jobId === job1.jobId || job.jobId === job2.jobId,
  );

  assertEquals(ourJobs.length >= 1, true, "At least one of our jobs should be in the list");

  // Clean up
  await mcpCancelJob({ queue: QUEUE_ID, jobId: job1.jobId, namespace: "mcp-test-list" });
  await mcpCancelJob({ queue: QUEUE_ID, jobId: job2.jobId, namespace: "mcp-test-list" });

  closeKv();
});

Deno.test("MCP: cancel a running job", async () => {
  // Submit a long-running job
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sleep 300", // 5 minutes
    namespace: "mcp-test-cancel",
  });

  console.log("Long-running job submitted:", result.jobId);

  // Wait a bit for it to start
  await new Promise((resolve) => setTimeout(resolve, 2000));

  // Check it's running or queued
  const statusBefore = await mcpGetJobStatus({
    queue: QUEUE_ID,
    jobId: result.jobId,
  });

  console.log("Status before cancellation:", statusBefore.status?.state);

  // Cancel it
  const cancelResult = await mcpCancelJob({
    queue: QUEUE_ID,
    jobId: result.jobId,
    namespace: "mcp-test-cancel",
  });

  console.log("Cancel result:", cancelResult);

  assertEquals(cancelResult.success, true);

  // Wait a bit for cancellation to take effect
  await new Promise((resolve) => setTimeout(resolve, 2000));

  // Check it's cancelled or finished
  const statusAfter = await mcpGetJobStatus({
    queue: QUEUE_ID,
    jobId: result.jobId,
  });

  console.log("Status after cancellation:", statusAfter.status?.state);

  // Should be Cancelled or Finished
  const terminalStates = ["Cancelled", "Finished"];
  assertEquals(
    terminalStates.includes(statusAfter.status?.state ?? ""),
    true,
    `Job should be in terminal state, got: ${statusAfter.status?.state}`,
  );

  closeKv();
});

Deno.test("MCP: monitor job with failure", async () => {
  // Submit a job that will fail
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sh -c 'echo Error message >&2; exit 1'",
    namespace: "mcp-test-failure",
  });

  console.log("Failing job submitted:", result.jobId);

  // Wait for completion
  const finalStatus = await waitForJobCompletion(QUEUE_ID, result.jobId, 60000);

  console.log("Final status:", JSON.stringify(finalStatus, null, 2));

  assertEquals(finalStatus.status?.state, "Finished");
  assertExists(finalStatus.result);
  assertEquals(finalStatus.result.statusCode, 1, "Job should have exit code 1");

  // Check failure reason
  assertEquals(finalStatus.finishedReason, "Failure", "Job should be marked as failed");

  closeKv();
});

Deno.test("MCP: poll job status multiple times", async () => {
  // Submit a job that takes a few seconds
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "sh -c 'echo Starting; sleep 3; echo Done'",
    namespace: "mcp-test-poll",
  });

  console.log("Job submitted:", result.jobId);

  const states: string[] = [];

  // Poll status multiple times
  for (let i = 0; i < 5; i++) {
    const status = await mcpGetJobStatus({
      queue: QUEUE_ID,
      jobId: result.jobId,
    });
    states.push(status.status?.state ?? "unknown");
    console.log(`Poll ${i + 1}: ${status.status?.state}`);

    if (status.status?.state === "Finished") {
      break;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  console.log("State progression:", states);

  // Should eventually reach Finished
  assertEquals(
    states[states.length - 1],
    "Finished",
    "Job should eventually finish",
  );

  closeKv();
});
