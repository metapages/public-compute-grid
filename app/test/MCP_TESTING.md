# MCP Server Testing Guide

This document describes the test infrastructure for the MCP (Model Context Protocol) server implementation.

## Overview

The MCP server enables LLMs to submit and monitor Docker container jobs through a standardized protocol. The test
infrastructure validates:

- Job submission with various configurations
- Job status monitoring and polling
- Job lifecycle management (queued → running → finished)
- Error handling and edge cases
- Integration workflows

## Test Files

### `mcp_util.ts`

Utility functions for testing MCP functionality:

- `sendMCPRequest()` - Send raw MCP requests
- `callMCPTool()` - Call MCP tools by name
- `listMCPTools()` - List available tools
- `mcpSubmitJob()` - Submit jobs via MCP
- `mcpGetJobStatus()` - Get job status via MCP
- `mcpListJobs()` - List jobs in a queue
- `mcpCancelJob()` - Cancel a job
- `waitForJobCompletion()` - Poll until job finishes

### `mcp_submit_job_test.ts`

Tests for job submission functionality:

- ✅ Submit simple Alpine job
- ✅ Submit job with inputs and environment variables
- ✅ Submit job with git repository
- ✅ Submit job with inline Dockerfile
- ✅ Submit job with maxDuration
- ✅ Error handling (no image/gitRepo)
- ✅ Error handling (both image and gitRepo)

### `mcp_job_monitoring_test.ts`

Tests for job monitoring and status:

- ✅ Monitor job status from queued to finished
- ✅ Get detailed job results with logs
- ✅ List jobs in queue
- ✅ Cancel a running job
- ✅ Monitor job with failure
- ✅ Poll job status multiple times

### `mcp_integration_test.ts`

End-to-end integration tests:

- ✅ List available MCP tools
- ✅ Complete workflow - build and run a Python app
- ✅ Build from git repository
- ✅ Multi-step workflow with environment variables
- ✅ Concurrent job submission
- ✅ Iterative container development simulation

## Running Tests

### Prerequisites

1. Start the local development stack:
   ```bash
   just dev local
   ```

2. Verify the stack is running:
   ```bash
   docker ps
   docker logs worker-metapage-worker-1
   ```

### Run All MCP Tests

```bash
# Run all MCP tests in local mode
just test local mcp_

# Run a specific test file
deno test --allow-all app/test/src/mcp_submit_job_test.ts

# Run a specific test case
deno test --allow-all --filter "MCP: submit simple Alpine job" app/test/src/mcp_submit_job_test.ts
```

### Run Individual Test Suites

```bash
# Job submission tests
deno test --allow-all app/test/src/mcp_submit_job_test.ts

# Job monitoring tests
deno test --allow-all app/test/src/mcp_job_monitoring_test.ts

# Integration tests
deno test --allow-all app/test/src/mcp_integration_test.ts
```

## Test Configuration

Tests use environment variables:

- `QUEUE_ID` - Queue name (default: "local1")
- `API_URL` - API endpoint (default: "http://worker:8000" for local mode)

When running in local mode:

- API_URL: `http://worker:8000`
- MCP endpoint: `http://worker:8000/mcp`

When running in remote mode:

- API_URL: `http://api1:8081`
- MCP endpoint: `http://api1:8081/mcp`

## Writing New Tests

### Basic Test Structure

```typescript
import { assertEquals, assertExists } from "std/assert";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { mcpGetJobStatus, mcpSubmitJob, QUEUE_ID } from "./mcp_util.ts";

Deno.test("MCP: your test name", async () => {
  // Submit a job
  const result = await mcpSubmitJob({
    queue: QUEUE_ID,
    image: "alpine:3.18.5",
    command: "echo 'test'",
    namespace: "test-namespace",
  });

  // Verify submission
  assertExists(result.jobId);
  assertEquals(result.success, true);

  // Get status
  const status = await mcpGetJobStatus({
    queue: QUEUE_ID,
    jobId: result.jobId,
  });

  // Assertions
  assertExists(status);

  // Cleanup
  closeKv();
});
```

### Testing Job Completion

```typescript
import { waitForJobCompletion } from "./mcp_util.ts";

const result = await mcpSubmitJob({
  queue: QUEUE_ID,
  image: "alpine:3.18.5",
  command: "echo 'test'",
});

// Wait for job to finish (max 60 seconds)
const finalStatus = await waitForJobCompletion(
  QUEUE_ID,
  result.jobId,
  60000,
);

assertEquals(finalStatus.state, "Finished");
```

### Testing Error Handling

```typescript
try {
  await mcpSubmitJob({
    queue: QUEUE_ID,
    // Missing required fields
    command: "echo test",
  });
  throw new Error("Should have thrown an error");
} catch (error) {
  assertExists(error.message);
  // Verify error message is helpful
}
```

## Common Test Patterns

### Job Submission Options

```typescript
// Simple job
await mcpSubmitJob({
  queue: QUEUE_ID,
  image: "alpine:3.18.5",
  command: "echo hello",
});

// Job with inputs
await mcpSubmitJob({
  queue: QUEUE_ID,
  image: "python:3.11-alpine",
  command: "python /inputs/script.py",
  inputs: {
    "script.py": "print('Hello')",
  },
});

// Job with environment variables
await mcpSubmitJob({
  queue: QUEUE_ID,
  image: "alpine:3.18.5",
  command: "echo $MY_VAR",
  env: {
    MY_VAR: "test_value",
  },
});

// Job from git repository
await mcpSubmitJob({
  queue: QUEUE_ID,
  gitRepo: "https://github.com/user/repo.git",
  command: "make build",
});

// Job with inline Dockerfile
await mcpSubmitJob({
  queue: QUEUE_ID,
  dockerfile: `
FROM alpine:3.18.5
RUN apk add --no-cache curl
`,
  command: "curl --version",
});
```

### Monitoring Patterns

```typescript
// Poll until completion
const final = await waitForJobCompletion(QUEUE_ID, jobId);

// Manual polling
let status;
do {
  status = await mcpGetJobStatus({ queue: QUEUE_ID, jobId });
  await new Promise((r) => setTimeout(r, 1000));
} while (status.state !== "Finished");

// Check logs
const logs = status.result?.logs?.map((l) => l[0]).join("") || "";
assertEquals(logs.includes("expected output"), true);
```

## Debugging Tests

### View Job Logs

```typescript
const status = await mcpGetJobStatus({ queue: QUEUE_ID, jobId });
console.log("Job status:", JSON.stringify(status, null, 2));

if (status.result?.logs) {
  const logs = status.result.logs.map((l) => l[0]).join("");
  console.log("Job output:", logs);
}
```

### Check Queue State

```typescript
const jobs = await mcpListJobs({ queue: QUEUE_ID });
console.log("Jobs in queue:", jobs);
```

### View Docker Logs

```bash
# Worker logs
docker logs worker-metapage-worker-1

# API logs (if using remote mode)
docker logs worker-metapage-api1-1
```

## CI/CD Integration

The MCP tests can be integrated into CI pipelines:

```bash
# Start services
just dev local

# Wait for services to be ready
sleep 10

# Run tests
just test local mcp_

# Cleanup
docker compose down
```

## Best Practices

1. **Always clean up**: Call `closeKv()` at the end of tests
2. **Use unique namespaces**: Prevents test interference
3. **Set timeouts**: Use `waitForJobCompletion()` with reasonable timeouts
4. **Test both success and failure**: Verify error handling
5. **Check logs**: Verify job output contains expected content
6. **Cancel long-running jobs**: Don't leave jobs running after tests

## Troubleshooting

### Tests Timeout

- Check if Docker is running: `docker ps`
- Check worker logs: `docker logs worker-metapage-worker-1`
- Increase timeout in `waitForJobCompletion()`

### Connection Refused

- Ensure `just dev local` is running
- Check API_URL environment variable
- Verify MCP endpoint: `curl http://worker:8000/mcp/health`

### Jobs Stuck in Queue

- Check worker capacity: `docker stats`
- Cancel stuck jobs manually
- Restart the worker: `docker restart worker-metapage-worker-1`

### Test Failures

- Run with verbose output: `deno test --allow-all --log-level=debug`
- Check individual job results via API
- Verify namespace isolation

## Future Enhancements

Potential improvements to the test infrastructure:

- [ ] Add performance benchmarks
- [ ] Test WebSocket MCP transport
- [ ] Add stress tests with many concurrent jobs
- [ ] Test MCP resources (not just tools)
- [ ] Add visual progress reporting
- [ ] Test with real-world complex repositories
- [ ] Add snapshot testing for job results
- [ ] Test MCP server reconnection logic

## Related Documentation

- [MCP Server README](../mcp/README.md)
- [API MCP Integration](../api/README-MCP.md)
- [Project README](../../README.md)
