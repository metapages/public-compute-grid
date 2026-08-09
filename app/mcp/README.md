# Worker Metapage MCP Server

Model Context Protocol (MCP) server for worker.metapage.io - enables AI agents to submit and manage Docker jobs through the queue system.

## Quick Start

### 1. Start the MCP Server

```bash
cd app/mcp
deno task start
```

### 2. Test the Server

```bash
deno task test
```

### 3. Use with Claude Desktop

Add to your Claude Desktop MCP config (copy from `claude-desktop-config.json`):

```json
{
  "mcpServers": {
    "worker-metapage": {
      "command": "deno",
      "args": [
        "run", 
        "--allow-net", 
        "--allow-env", 
        "--allow-read",
        "--allow-write", 
        "/Users/dion/dev/git/metapages/worker.metapage.io/app/mcp/src/server.ts"
      ],
      "env": {
        "WORKER_METAPAGE_URL": "https://container.mtfm.io"
      }
    }
  }
}
```

## Available Tools

- **submit_job** - Submit Docker jobs to queues
- **get_job_status** - Check job status and results  
- **list_jobs** - List jobs in a queue
- **cancel_job** - Cancel running jobs
- **upload_file** - Upload files for job inputs

## Available Resources

- **queue://{name}/jobs** - Live job listings
- **job://status/template** - Job status check template

## Example Usage

Ask Claude:
- "Submit a Python job to analyze some data in the public1 queue"
- "Check the status of my recent jobs"
- "List all running jobs in my-queue"

## Environment Variables

- `WORKER_METAPAGE_URL` - API base URL (default: https://container.mtfm.io)