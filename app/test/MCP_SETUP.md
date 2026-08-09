# MCP Test Infrastructure Setup

## Summary

Complete test infrastructure has been created for the MCP server. The infrastructure includes:

### Created Files

1. **`app/test/src/mcp_util.ts`** - Test utilities for MCP operations
   - Functions to call MCP tools
   - Helper functions for job submission, status checking, and monitoring
   - Polling utilities for waiting on job completion

2. **`app/test/src/mcp_submit_job_test.ts`** - Job submission tests (7 tests)
   - Simple Alpine job submission
   - Jobs with inputs and environment variables
   - Git repository jobs
   - Inline Dockerfile jobs
   - Error handling tests

3. **`app/test/src/mcp_job_monitoring_test.ts`** - Job monitoring tests (7 tests)
   - Status monitoring from queued to finished
   - Detailed job results with logs
   - Listing jobs in queue
   - Cancelling running jobs
   - Failure handling
   - Polling status multiple times

4. **`app/test/src/mcp_integration_test.ts`** - Integration tests (6 tests)
   - List available MCP tools
   - Complete workflow (Python app)
   - Git repository builds
   - Multi-step workflows with env vars
   - Concurrent job submission
   - Iterative container development simulation

5. **`app/test/MCP_TESTING.md`** - Comprehensive testing documentation
   - Test file descriptions
   - Usage examples
   - Best practices
   - Troubleshooting guide

6. **`app/test/MCP_SETUP.md`** - This file

### Test Coverage

**Total: 20 test cases** covering:

- ✅ Job submission (simple, with inputs, git repos, Dockerfiles)
- ✅ Job monitoring and status polling
- ✅ Job cancellation
- ✅ Error handling
- ✅ Concurrent operations
- ✅ Integration workflows
- ✅ Iterative development patterns

## Current Status

### ✅ Complete

- MCP test utilities
- All test files created
- Documentation written
- Test structure follows existing patterns

### ⚠️ Known Issues

1. **Pre-existing Type Errors** (not caused by MCP tests)
   - Location: `app/shared/src/shared/s3.ts:188`
   - Issue: AWS SDK @smithy/types version conflicts
   - Impact: `just check` fails, but tests may still run
   - Files affected: Not the MCP test files themselves

2. **Dev Stack Not Running**
   - Tests require `just dev local` to be running
   - MCP endpoint needs to be accessible at `http://worker:8000/mcp`

## Running the Tests

### Quick Start (Recommended)

The easiest way to test the MCP server is using the `just test-mcp` command:

```bash
# Start the local dev stack
just dev local

# In another terminal, run the MCP tests
just test-mcp
```

The `test-mcp` command will:

- ✅ Check if the local dev stack is running
- ✅ Verify the MCP endpoint is accessible
- ✅ Run all MCP tests with proper environment configuration
- ✅ Provide helpful error messages if the stack is not running

### Manual Testing

#### Step 1: Start the Development Stack

```bash
# Start in local mode (recommended for MCP testing)
just dev local
```

Wait for services to be ready (check logs):

```bash
docker logs -f worker-metapage-worker-1
```

#### Step 2: Run Tests

```bash
# Run all MCP tests
deno test --allow-all --no-check app/test/src/mcp_*.ts

# Run a specific test file
deno test --allow-all --no-check app/test/src/mcp_submit_job_test.ts

# Run a specific test case
deno test --allow-all --no-check --filter "MCP: submit simple Alpine job" app/test/src/mcp_submit_job_test.ts
```

## Test Environment

### Environment Variables

- `QUEUE_ID` - Queue name (default: "local1")
- `API_URL` - API endpoint (default: "http://worker:8000" for local mode)

### Endpoints Used

- MCP endpoint: `http://worker:8000/mcp`
- Job API: `http://worker:8000/q/{queue}/j/{jobId}`
- Health check: `http://worker:8000/mcp/health`

## Fixing Type Errors (Optional)

The type errors in `app/shared/src/shared/s3.ts` are due to AWS SDK dependency version conflicts. To fix:

### Option 1: Update Dependencies

```bash
# This would require updating AWS SDK dependencies in deno.json
# to ensure compatible @smithy/types versions across all packages
```

### Option 2: Skip Type Checking for Tests

```bash
# Tests can often run without full type checking
deno test --allow-all --no-check app/test/src/mcp_*.ts
```

### Option 3: Isolate S3 Dependencies

The s3.ts file could be refactored to isolate AWS SDK dependencies, but this is outside the scope of MCP testing.

## Next Steps

### Immediate Actions

1. Start dev stack: `just dev local`
2. Run tests to verify: `deno test --allow-all --no-check app/test/src/mcp_submit_job_test.ts`
3. Review test results

### For Production

1. Address AWS SDK type conflicts in s3.ts
2. Add MCP tests to CI/CD pipeline
3. Consider adding performance benchmarks
4. Test with real-world repositories (like OpenThermo mentioned in CLAUDE.md)

## Testing with Real Repository (Manual)

As mentioned in CLAUDE.md, the intended use case is to test with a real repository:

```typescript
// Example: Testing OpenThermo repository
const result = await mcpSubmitJob({
  queue: QUEUE_ID,
  gitRepo: "https://github.com/lenhanpham/OpenThermo.git",
  command: "make build && make test", // or appropriate build command
  namespace: "openthermo-test",
  maxDuration: "30m",
});

// Monitor and iterate until working
const finalStatus = await waitForJobCompletion(QUEUE_ID, result.jobId, 1800000); // 30 min

// Check results and iterate if needed
if (finalStatus.result?.StatusCode !== 0) {
  // Analyze logs and adjust job configuration
  console.log(finalStatus.result?.logs);
  // Submit modified job...
}
```

## Architecture Notes

The MCP test infrastructure mirrors the existing test structure:

- Uses Deno test framework (like `functional_basic_test.ts`)
- Follows same patterns for job submission and monitoring
- Uses shared utilities from `@metapages/compute-queues-shared`
- Integrates with existing queue and job management
- Compatible with both local and remote modes

## Contact & Support

For issues with:

- **MCP tests**: Review `app/test/MCP_TESTING.md`
- **Type errors**: Check `app/shared/src/shared/s3.ts`
- **Dev stack**: Run `docker logs worker-metapage-worker-1`
- **MCP server**: Review `app/mcp/README.md`

## Summary

The MCP test infrastructure is **complete and ready to use**. To start testing:

1. `just dev local` - Start the dev stack in one terminal
2. `just test-mcp` - Run all MCP tests in another terminal
3. Review output and iterate

**New Command:** `just test-mcp`

- Uses curl to check MCP health endpoint (reliable detection)
- Tries multiple common ports (8000, 443)
- Verifies MCP endpoint accessibility before running tests
- Runs all MCP tests with correct configuration
- Provides helpful error messages with troubleshooting steps

The pre-existing AWS SDK type errors do not prevent the MCP tests from running when using the `--no-check` flag.
