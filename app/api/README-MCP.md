# MCP Integration for worker.metapage.io

The worker.metapage.io API now includes integrated MCP (Model Context Protocol) support, providing both HTTP and
WebSocket interfaces for AI agents to interact with the job queue system.

## Features

✅ **HTTP MCP Protocol Support** - Standard request/response tool calls\
✅ **WebSocket MCP Protocol Support** - Real-time streaming and notifications\
✅ **Real-time Job Log Streaming** - Stream live job execution logs\
✅ **Job Status Notifications** - Get notified when job status changes\
✅ **Job Subscriptions** - Subscribe to specific job events\
✅ **Integrated with Existing Queue System** - No separate server needed

## Endpoints

When running the API server (default: `localhost:8000`):

- **HTTP MCP**: `POST http://localhost:8000/mcp`
- **WebSocket MCP**: `ws://localhost:8000/mcp/ws`
- **Health Check**: `GET http://localhost:8000/mcp/health`
- **Server Info**: `GET http://localhost:8000/mcp/info`

## Available Tools

1. **`submit_job`** - Submit Docker jobs to queues
2. **`get_job_status`** - Check job status and results
3. **`list_jobs`** - List jobs in a queue with filtering
4. **`cancel_job`** - Cancel running or queued jobs
5. **`stream_job_logs`** - Stream real-time job logs (WebSocket)
6. **`subscribe_to_job`** - Subscribe to job events (WebSocket)

## Available Resources

- **`queue://{name}/jobs`** - Live job listings from any queue
- **`job://{jobId}/status`** - Real-time job status and results
- **`system://queues`** - List of all active queues
- **`system://workers`** - List of all active workers

## Usage with Claude Desktop

### 1. Start the API Server

```bash
cd app/api
just dev
# or
deno run --allow-all src/server.ts
```

### 2. Configure Claude Desktop

Add to your Claude Desktop MCP config file:

**Location:**

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%/Claude/claude_desktop_config.json`
- Linux: `~/.config/Claude/claude_desktop_config.json`

**Configuration:**

```json
{
  "mcpServers": {
    "worker-metapage": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

### 3. Restart Claude Desktop

Completely quit and restart Claude Desktop to load the MCP server.

### 4. Test the Integration

Try these prompts with Claude:

- "What MCP tools do you have available?"
- "Submit a simple hello world job to the public1 queue"
- "List all jobs in the public1 queue"
- "Check the status of job xyz123"
- "Submit a Python job that analyzes some data"

## Real-time Features (WebSocket)

The WebSocket interface provides enhanced real-time capabilities:

### Job Log Streaming

```typescript
// Subscribe to real-time job logs
const subscription = await mcpClient.call("subscribe_to_job", {
  jobId: "your-job-id",
  events: ["logs", "status", "completion"],
});

// Receive streaming updates
mcpClient.onNotification("job/logs", (data) => {
  console.log(`[${data.jobId}] ${data.data.logs.join("\n")}`);
});
```

### Job Status Notifications

```typescript
// Get notified when job status changes
mcpClient.onNotification("job/status_changed", (data) => {
  console.log(`Job ${data.jobId} is now ${data.data.newState}`);
});

// Get notified when job completes
mcpClient.onNotification("job/completed", (data) => {
  console.log(`Job ${data.jobId} finished: ${data.data.finalState}`);
});
```

## Development and Testing

### Test the MCP Server

```bash
cd app/api
deno run --allow-net --allow-env --allow-read src/routes/mcp/test.ts
```

### Manual HTTP Testing

```bash
# Test health
curl http://localhost:8000/mcp/health

# Test server info
curl http://localhost:8000/mcp/info

# Test tools/list
curl -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

### WebSocket Testing

Use a WebSocket client to connect to `ws://localhost:8000/mcp/ws` and send MCP JSON-RPC messages.

## Architecture

The MCP integration is built directly into the existing Hono API server:

```
┌─────────────────┐    HTTP/WS     ┌─────────────────┐    Internal     ┌─────────────────┐
│   Claude AI     │  ◄──────────►  │  Hono Server    │  ◄──────────►  │ Job Queue       │
│   (MCP Client)  │                │  (MCP Endpoint) │                │ System          │
└─────────────────┘                └─────────────────┘                └─────────────────┘
                                           │                                    │
                                           ▼                                    ▼
                                   Real-time notifications           WebSocket job logs
                                   via WebSocket streams             and status updates
```

The MCP server integrates seamlessly with the existing job queue infrastructure, providing AI agents with powerful
access to containerized compute resources.
