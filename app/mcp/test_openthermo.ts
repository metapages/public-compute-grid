#!/usr/bin/env -S deno run --allow-all

import { WorkerMetapageClient } from "./src/client.ts";
import { handleSubmitJob, submitJobTool } from "./src/tools/submit-job.ts";

// Test the OpenThermo example from CLAUDE.md
const client = new WorkerMetapageClient("http://localhost:8000");

// Create a mock request for the OpenThermo repository
const mockRequest = {
  name: "submit_job",
  params: {
    arguments: {
      queue: "dev",
      gitRepo: "https://github.com/lenhanpham/OpenThermo",
      buildContext: ".",
      command: "echo 'OpenThermo repository cloned and built successfully!'",
      env: {
        "BUILD_ENV": "test",
      },
      maxDuration: "10m",
      namespace: "dev-test",
    },
  },
};

console.log("🧪 Testing OpenThermo job submission...");
console.log("📋 Request:", JSON.stringify(mockRequest, null, 2));

try {
  const result = await handleSubmitJob(mockRequest as any, client);
  console.log("✅ Result:", JSON.stringify(result, null, 2));
} catch (error) {
  console.error("❌ Error:", error);
}
