import { assertEquals, assertExists } from "std/assert";

import {
  createNewContainerJobMessage,
  ENV_VAR_DATA_ITEM_LENGTH_MAX,
  fetchRobust,
  fileToDataref,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL")!;

const originalFetch = globalThis.fetch;

const fetch = fetchRobust;

Deno.test(
  "REMOTE: Test job inputs and outputs file access via redirect",
  async () => {
    if (QUEUE_ID !== "local1") {
      console.log("Skipping test - remote mode only");
      return;
    }

    // Create a temporary test file for input
    let testInputContent = `Hello from test input file!
This is a test file for validating job input access.
Timestamp: ${Date.now()}`;
    while (testInputContent.length < ENV_VAR_DATA_ITEM_LENGTH_MAX) {
      testInputContent += testInputContent;
    }

    const testInputFile = `/tmp/test_input_${Date.now()}.txt`;
    await Deno.writeTextFile(testInputFile, testInputContent);

    try {
      // Convert file to dataref
      const inputDataref = await fileToDataref(testInputFile, API_URL);

      const definition = {
        image: "alpine:3.18.5",
        command:
          `sh -c 'echo "Processing input file..." && cat /inputs/test_input.txt > /outputs/test_output.txt && echo "Job completed successfully"'`,
        inputs: {
          "test_input.txt": inputDataref,
        },
      };

      const { queuedJob, jobId } = await createNewContainerJobMessage({
        definition,
      });

      if (!queuedJob) {
        throw new Error("Failed to create job message");
      }

      // Submit the job using the EnqueueJob structure
      const submitResponse = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
        method: "POST",
        body: JSON.stringify(queuedJob.enqueued),
        headers: {
          "Content-Type": "application/json",
        },
      });
      assertEquals(submitResponse.status, 200);
      const submitBody = await submitResponse.json();
      assertEquals(submitBody.success, true);
      assertExists(submitBody.jobId);

      // Wait for job to complete by polling the job status
      let jobCompleted = false;
      let attempts = 0;
      const maxAttempts = 60; // 60 seconds timeout

      while (!jobCompleted && attempts < maxAttempts) {
        const jobResponse = await fetch(`${API_URL}/j/${jobId}.json`);
        assertEquals(jobResponse.status, 200);
        const jobData = await jobResponse.json();
        const state = jobData.data?.results?.state;
        if (state === "Finished") {
          jobCompleted = true;
          assertEquals(jobData.data.results.finishedReason, "Success");
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second
          attempts++;
        }
      }

      assertEquals(jobCompleted, true, "Job should have completed within timeout");

      // Test input file access via redirect
      const inputResponse = await fetch(`${API_URL}/j/${jobId}/inputs/test_input.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });

      assertEquals(inputResponse.status, 302, "Should return 302 redirect");
      assertExists(inputResponse.headers.get("location"), "Should have location header");
      inputResponse.body?.cancel();

      // Follow the redirect to get the actual file content
      const inputFileResponse = await fetch(inputResponse.headers.get("location")!);

      const inputFileContent = await inputFileResponse.text();
      assertEquals(inputFileResponse.status, 200);
      assertEquals(inputFileContent, testInputContent);

      // Test output file access via redirect
      const outputResponse = await fetch(`${API_URL}/j/${jobId}/outputs/test_output.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });
      outputResponse.body?.cancel();

      assertEquals(outputResponse.status, 302, "Should return 302 redirect");
      assertExists(outputResponse.headers.get("location"), "Should have location header");

      // Follow the redirect to get the actual file content
      const outputFileResponse = await fetch(outputResponse.headers.get("location")!);
      assertEquals(outputFileResponse.status, 200);
      const outputFileContent = await outputFileResponse.text();
      assertEquals(outputFileContent, testInputContent); // Should be the same as input

      // Test queue-based endpoints as well
      const queueInputResponse = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/inputs/test_input.txt`, {
        redirect: "manual",
      });
      queueInputResponse.body?.cancel();
      assertEquals(queueInputResponse.status, 302);
      assertExists(queueInputResponse.headers.get("location"), "Should have location header");
      const queueInputFileResponse = await fetch(queueInputResponse.headers.get("location")!);
      const queueInputFileContent = await queueInputFileResponse.text();
      assertEquals(queueInputFileContent, testInputContent);

      const queueOutputResponse = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/test_output.txt`, {
        redirect: "manual",
      });
      queueOutputResponse.body?.cancel();
      assertEquals(queueOutputResponse.status, 302);
      assertExists(queueOutputResponse.headers.get("location"), "Should have location header");

      const queueOutputFileResponse = await fetch(queueOutputResponse.headers.get("location")!);
      const queueOutputFileContent = await queueOutputFileResponse.text();
      assertEquals(queueOutputFileContent, testInputContent);

      // Test error cases
      const nonExistentInputResponse = await originalFetch(`${API_URL}/j/${jobId}/inputs/nonexistent.txt`);
      assertEquals(nonExistentInputResponse.status, 404);
      nonExistentInputResponse.body?.cancel();

      const nonExistentOutputResponse = await originalFetch(`${API_URL}/j/${jobId}/outputs/nonexistent.txt`);
      assertEquals(nonExistentOutputResponse.status, 404);
      nonExistentOutputResponse.body?.cancel();
    } finally {
      // Clean up test file
      try {
        await Deno.remove(testInputFile);
      } catch {
        // Ignore cleanup errors
      }
    }

    closeKv();
  },
);

Deno.test(
  "LOCAL: Test job inputs and outputs file access via redirect",
  async () => {
    if (QUEUE_ID !== "local") {
      console.log("Skipping test - local mode only");
      return;
    }
    // Create a temporary test file for input
    let testInputContent = `Hello from test input file!
This is a test file for validating job input access.
Timestamp: ${Date.now()}`;
    while (testInputContent.length < ENV_VAR_DATA_ITEM_LENGTH_MAX) {
      testInputContent += testInputContent;
    }

    const testInputFile = `/tmp/test_input_${Date.now()}.txt`;
    await Deno.writeTextFile(testInputFile, testInputContent);

    try {
      // Convert file to dataref
      const inputDataref = await fileToDataref(testInputFile, API_URL);

      const definition = {
        image: "alpine:3.18.5",
        command:
          `sh -c 'echo "Processing input file..." && cat /inputs/test_input.txt > /outputs/test_output.txt && echo "Job completed successfully"'`,
        inputs: {
          "test_input.txt": inputDataref,
        },
      };

      const { queuedJob, jobId } = await createNewContainerJobMessage({
        definition,
      });

      if (!queuedJob) {
        throw new Error("Failed to create job message");
      }

      // Submit the job using the EnqueueJob structure
      const submitResponse = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
        method: "POST",
        body: JSON.stringify(queuedJob.enqueued),
        headers: {
          "Content-Type": "application/json",
        },
      });
      assertEquals(submitResponse.status, 200);
      const submitBody = await submitResponse.json();
      assertEquals(submitBody.success, true);
      assertExists(submitBody.jobId);

      // Wait for job to complete by polling the job status
      let jobCompleted = false;
      let attempts = 0;
      const maxAttempts = 60; // 60 seconds timeout

      while (!jobCompleted && attempts < maxAttempts) {
        const jobResponse = await fetch(`${API_URL}/j/${jobId}.json`);
        assertEquals(jobResponse.status, 200);
        const jobData = await jobResponse.json();
        const state = jobData.data?.results?.state;
        if (state === "Finished") {
          jobCompleted = true;
          assertEquals(jobData.data.results.finishedReason, "Success");
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second
          attempts++;
        }
      }

      assertEquals(jobCompleted, true, "Job should have completed within timeout");

      // Test input file access
      const inputResponse = await fetch(`${API_URL}/j/${jobId}/inputs/test_input.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });

      assertEquals(inputResponse.status, 200, "Should return 200");
      const inputFileContent = await inputResponse.text();
      assertEquals(inputFileContent, testInputContent);

      // Test output file access
      const outputResponse = await fetch(`${API_URL}/j/${jobId}/outputs/test_output.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });

      assertEquals(outputResponse.status, 200, "Should return 200 redirect");
      const outputFileContent = await outputResponse.text();
      assertEquals(outputFileContent, testInputContent); // Should be the same as input

      // Test queue-based endpoints as well
      const queueInputResponse = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/inputs/test_input.txt`, {
        redirect: "manual",
      });
      assertEquals(queueInputResponse.status, 200);
      const queueInputFileContent = await queueInputResponse.text();
      assertEquals(queueInputFileContent, testInputContent);

      const queueOutputResponse = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/test_output.txt`, {
        redirect: "manual",
      });
      assertEquals(queueOutputResponse.status, 200);
      const queueOutputFileContent = await queueOutputResponse.text();
      assertEquals(queueOutputFileContent, testInputContent);

      // Test error cases
      const nonExistentInputResponse = await originalFetch(`${API_URL}/j/${jobId}/inputs/nonexistent.txt`);
      assertEquals(nonExistentInputResponse.status, 404);
      nonExistentInputResponse.body?.cancel();

      const nonExistentOutputResponse = await originalFetch(`${API_URL}/j/${jobId}/outputs/nonexistent.txt`);
      assertEquals(nonExistentOutputResponse.status, 404);
      nonExistentOutputResponse.body?.cancel();
    } finally {
      // Clean up test file
      try {
        await Deno.remove(testInputFile);
      } catch {
        // Ignore cleanup errors
      }
    }

    closeKv();
  },
);
