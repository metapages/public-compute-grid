# MCP Integration for worker.metapage.io

The API server mounts an MCP (Model Context Protocol) server, so an agent can build and run containers on a compute
queue over the same origin that serves the queue API. The local-mode worker (`just app/worker/local`) mounts the same
server, so a local stack needs nothing extra.

The server itself lives in `app/mcp` (`createMcpServer`), and is the same one the stdio entrypoint serves. There is one
tool list, in `app/mcp/src/server-factory.ts`.

## Endpoints

When running the API server (default: `localhost:8000`):

- **MCP (Streamable HTTP)**: `http://localhost:8000/mcp` — POST for messages, GET for the notification stream that
  carries live logs, DELETE to end a session. Stateless, so any server instance can serve any request.
- **Health Check**: `GET http://localhost:8000/mcp/health`
- **Server Info**: `GET http://localhost:8000/mcp/info` — generated from the tool list, so it cannot go stale.

There is no websocket transport. `/mcp/ws` served a second MCP implementation that read in-process queue state across
every queue; it was never served by the local-mode worker, and it was retired during the Cloudflare migration (issue
#308), where one queue lives in one Durable Object. Use Streamable HTTP.

## Available Tools

1. **`submit_job`** — Submit a Docker job (an image, or a Dockerfile/git repo to build) with inputs and env vars
2. **`follow_job`** — Follow a job to completion, streaming build and run logs as progress notifications
3. **`get_job_status`** — Check a job's state and, once finished, its result
4. **`list_jobs`** — List jobs in a queue
5. **`cancel_job`** — Cancel a running or queued job
6. **`upload_file`** — Upload a file and get a key usable as a job input

`follow_job` is the one to reach for while a job runs: it streams logs instead of polling `get_job_status` in a loop.

## Available Resources

- **`queue://{name}/jobs`** — job listings from a queue

## Usage with Claude Desktop or Claude Code

### 1. Start a server

```bash
# the API server
cd app/api && just dev

# or a local-mode worker, which serves the same API and MCP endpoint on :8000
just app/worker/local
```

### 2. Configure the client

```json
{
  "mcpServers": {
    "worker-metapage": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

Production is the same URL shape: `https://container.mtfm.io/mcp`.

### 3. Restart the client

Completely quit and restart Claude Desktop to load the MCP server.

### 4. Test the integration

Try these prompts:

- "What MCP tools do you have available?"
- "Submit a hello world job to the public1 queue and follow it"
- "List all jobs in the public1 queue"
- "Build a container from this git repo and get it running"

## Development and Testing

```bash
# Health and info
curl http://localhost:8000/mcp/health
curl http://localhost:8000/mcp/info

# tools/list over Streamable HTTP. The reply is SSE-framed ("event: message"),
# which is what the Accept header asks for; a real client sends it too.
curl -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

The functional suite covers this surface against a real stack: `app/test/src/mcp_*_test.ts`, run by `just test`.

## Architecture

```
┌─────────────────┐  Streamable HTTP  ┌──────────────────┐   HTTP    ┌─────────────────┐
│   MCP client    │  ◄─────────────►  │  /mcp (app/mcp)  │  ◄─────►  │  queue API      │
│  (agent)        │                   │  mounted in the  │           │  (same origin)  │
└─────────────────┘                   │  API or worker   │           └─────────────────┘
                                      └──────────────────┘                    │
                                               │                              ▼
                                      progress notifications          workers run the
                                      carry live build/run logs       containers
```

The tools talk to the queue API over HTTP rather than reaching into the server's memory, which is why the same server
works mounted in the API, in the local-mode worker, and as a standalone stdio server.
