#!/usr/bin/env deno run --allow-net --allow-env --allow-read

import { WorkerMetapageClient } from "./client.ts";

/**
 * Test script to verify the MCP server components work correctly
 */
async function testClient() {
  console.log("🧪 Testing Worker Metapage Client...");

  const client = new WorkerMetapageClient("https://container.mtfm.io");

  try {
    // Test job submission
    console.log("📤 Testing job submission...");
    const jobResult = await client.submitJob("test-mcp-queue", {
      definition: {
        image: "alpine:latest",
        command: "echo 'Hello from MCP test!'",
        inputs: {
          "test.txt": "This is a test file from MCP",
        },
      },
    });

    console.log("✅ Job submitted:", jobResult);

    // Test job status
    if (jobResult.jobId) {
      console.log("📊 Testing job status...");
      const status = await client.getJobStatus(jobResult.jobId);
      console.log("✅ Job status:", status);
    }

    // Test list jobs
    console.log("📋 Testing list jobs...");
    const jobs = await client.listJobs("test-mcp-queue");
    console.log("✅ Jobs listed:", Object.keys(jobs.jobs || {}).length, "jobs found");
  } catch (error) {
    console.error("❌ Test failed:", (error as Error).message);
    console.error("Stack:", (error as Error).stack);
  }
}

async function testMCPServerInfo() {
  console.log("🔍 MCP Server Information:");
  console.log("  Base URL:", Deno.env.get("WORKER_METAPAGE_URL") || "https://container.mtfm.io");
  console.log("  Tools: submit_job, get_job_status, list_jobs, cancel_job, upload_file");
  console.log("  Resources: queue://{name}/jobs, job://status/template");
  console.log("");
  console.log("💡 To test with Claude:");
  console.log("  1. Add this MCP server to your Claude desktop config");
  console.log("  2. Use the tools to submit and monitor Docker jobs");
  console.log("  3. Try: 'Submit a simple hello world job to the public1 queue'");
}

if (import.meta.main) {
  await testMCPServerInfo();
  await testClient();
}
