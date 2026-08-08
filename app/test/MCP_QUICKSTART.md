# MCP Server Testing - Quick Start

This guide shows you how to quickly test the MCP server using the new `just test-mcp` command.

## Prerequisites

- Docker running
- Just command runner installed

## Testing the MCP Server

### 1. Start the Local Dev Stack

In one terminal:

```bash
just dev local
```

Wait for the stack to start. You should see logs indicating the worker is ready.

### 2. Run MCP Tests

In another terminal:

```bash
just test-mcp
```

### Expected Output

```
Checking if local dev stack is running...
✅ Local dev stack detected: worker-metapage-worker-1

Checking MCP endpoint...
✅ MCP endpoint is accessible

Running MCP tests...
  QUEUE_ID: local
  API_URL: http://worker:8000

running 20 tests from ./src/mcp_submit_job_test.ts
MCP: submit simple Alpine job ... ok (2s)
MCP: submit job with inputs and env vars ... ok (1s)
...

✅ MCP tests completed successfully!
```

## What if the Stack is Not Running?

If you run `just test-mcp` without starting the dev stack first, you'll get a helpful error:

```
❌ Error: MCP server health endpoint not responding!

Tried:
  - http://localhost:8000/mcp/health
  - http://localhost:443/mcp/health

Please start the local stack first:
  just dev local

Then verify it's running:
  curl http://localhost:8000/mcp/health
```

The command uses curl to check the MCP health endpoint, which is more reliable than checking Docker container names.

## Test Details

The `just test-mcp` command runs **20 test cases** covering:

- ✅ Job submission (simple, with inputs, git repos, Dockerfiles)
- ✅ Job monitoring and status polling
- ✅ Job cancellation
- ✅ Error handling
- ✅ Concurrent operations
- ✅ Integration workflows
- ✅ Iterative development patterns

## Running Specific Tests

If you want to run specific test files manually:

```bash
# Single test file
deno test --allow-all --no-check app/test/src/mcp_submit_job_test.ts

# Specific test case
deno test --allow-all --no-check --filter "MCP: submit simple Alpine job" app/test/src/mcp_submit_job_test.ts
```

## Troubleshooting

### Tests Fail to Connect

Check if the stack is running:

```bash
docker ps | grep worker-metapage
```

### MCP Endpoint Not Responding

Check worker logs:

```bash
docker logs worker-metapage-worker-1
```

### Port Conflicts

If port 8000 is already in use, the local stack may fail to start. Check the `just dev local` output for errors.

## Next Steps

After verifying the MCP server works with the test suite, you can:

1. **Test with real repositories** - Use the patterns in `mcp_integration_test.ts` as examples
2. **Connect Claude Code** - Configure the MCP server in your Claude Code settings
3. **Iterate on container builds** - Use the MCP tools to iteratively fix and test containers

## Related Documentation

- [MCP_TESTING.md](./MCP_TESTING.md) - Comprehensive testing documentation
- [MCP_SETUP.md](./MCP_SETUP.md) - Detailed setup instructions
- [../mcp/README.md](../mcp/README.md) - MCP server documentation
