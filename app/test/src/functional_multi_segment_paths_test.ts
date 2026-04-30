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
  "Test job inputs and outputs with multi-segment file paths",
  async () => {
    // Create a temporary test file for input with nested path
    let testInputContent = `Hello from nested test input file!
This is a test file for validating job input access with multi-segment paths.
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
          `sh -c 'mkdir -p /outputs/nested/subfolder && echo "Processing nested input file..." && cat /inputs/nested/subfolder/test_input.txt > /outputs/nested/subfolder/test_output.txt && echo "Job completed successfully"'`,
        inputs: {
          "nested/subfolder/test_input.txt": inputDataref,
        },
      };

      const { queuedJob, jobId } = await createNewContainerJobMessage({
        definition,
      });

      // Submit the job
      const response = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(queuedJob?.enqueued),
      });

      assertEquals(response.status, 200);
      response.body?.cancel();

      // Wait for job to complete
      let jobCompleted = false;
      let attempts = 0;
      const maxAttempts = 60; // 60 seconds timeout

      while (!jobCompleted && attempts < maxAttempts) {
        const jobResponse = await fetch(`${API_URL}/j/${jobId}`);
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

      // Test input file access with multi-segment path
      const inputResponse = await fetch(`${API_URL}/j/${jobId}/inputs/nested/subfolder/test_input.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });

      let inputFileResponse: Response | undefined;

      if (QUEUE_ID === "local") {
        inputFileResponse = inputResponse;
      } else {
        assertEquals(inputResponse.status, 302, "Should return 302 redirect");
        assertExists(inputResponse.headers.get("location"), "Should have location header");
        inputResponse.body?.cancel();
        // Follow the redirect to get the actual file content
        inputFileResponse = await fetch(inputResponse.headers.get("location")!);
      }

      const inputFileContent = await inputFileResponse!.text();
      assertEquals(inputFileResponse!.status, 200);
      if (inputFileContent !== testInputContent) {
        console.log(
          `DEBUG: inputFileContent.length=${inputFileContent.length}, testInputContent.length=${testInputContent.length}`,
        );
        console.log(`DEBUG: inputFileContent first 200 chars: ${JSON.stringify(inputFileContent.substring(0, 200))}`);
        console.log(`DEBUG: testInputContent first 200 chars: ${JSON.stringify(testInputContent.substring(0, 200))}`);
        console.log(
          `DEBUG: inputFileContent last 50 chars: ${
            JSON.stringify(inputFileContent.substring(inputFileContent.length - 50))
          }`,
        );
        console.log(
          `DEBUG: testInputContent last 50 chars: ${
            JSON.stringify(testInputContent.substring(testInputContent.length - 50))
          }`,
        );
      }
      assertEquals(inputFileContent, testInputContent);

      // Test output file access with multi-segment path
      const outputResponse = await fetch(`${API_URL}/j/${jobId}/outputs/nested/subfolder/test_output.txt`, {
        redirect: "manual", // Don't follow redirects automatically
      });

      let outputFileResponse: Response | undefined;
      if (QUEUE_ID === "local") {
        outputFileResponse = outputResponse;
      } else {
        assertEquals(outputResponse.status, 302, "Should return 302 redirect");
        assertExists(outputResponse.headers.get("location"), "Should have location header");
        outputResponse.body?.cancel();
        // Follow the redirect to get the actual file content
        outputFileResponse = await fetch(outputResponse.headers.get("location")!);
      }

      assertEquals(outputFileResponse!.status, 200);
      const outputFileContent = await outputFileResponse!.text();
      assertEquals(outputFileContent, testInputContent); // Should be the same as input

      // Test queue-based endpoints with multi-segment paths as well
      const queueInputResponse = await fetch(
        `${API_URL}/q/${QUEUE_ID}/j/${jobId}/inputs/nested/subfolder/test_input.txt`,
        {
          redirect: "manual",
        },
      );

      let queueInputFileResponse: Response | undefined;
      if (QUEUE_ID === "local") {
        queueInputFileResponse = queueInputResponse;
      } else {
        assertEquals(queueInputResponse.status, 302, "Should return 302 redirect");
        assertExists(queueInputResponse.headers.get("location"), "Should have location header");
        queueInputResponse.body?.cancel();
        // Follow the redirect to get the actual file content
        queueInputFileResponse = await fetch(queueInputResponse.headers.get("location")!);
      }

      assertEquals(queueInputFileResponse!.status, 200, "Should return 200");

      const queueInputFileContent = await queueInputFileResponse!.text();
      assertEquals(queueInputFileContent, testInputContent);

      const queueOutputResponse = await fetch(
        `${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/nested/subfolder/test_output.txt`,
        {
          redirect: "manual",
        },
      );

      let queueOutputFileResponse: Response | undefined;
      if (QUEUE_ID === "local") {
        queueOutputFileResponse = queueOutputResponse;
      } else {
        assertEquals(queueOutputResponse.status, 302, "Should return 302 redirect");
        assertExists(queueOutputResponse.headers.get("location"), "Should have location header");
        queueOutputResponse.body?.cancel();
        // Follow the redirect to get the actual file content
        queueOutputFileResponse = await fetch(queueOutputResponse.headers.get("location")!);
      }

      const queueOutputFileContent = await queueOutputFileResponse!.text();
      assertEquals(queueOutputFileContent, testInputContent);

      // Test error cases with multi-segment paths
      const nonExistentInputResponse = await originalFetch(
        `${API_URL}/j/${jobId}/inputs/nested/subfolder/nonexistent.txt`,
      );
      assertEquals(nonExistentInputResponse.status, 404);
      nonExistentInputResponse.body?.cancel();

      const nonExistentOutputResponse = await originalFetch(
        `${API_URL}/j/${jobId}/outputs/nested/subfolder/nonexistent.txt`,
      );
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
