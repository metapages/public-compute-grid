/**
 * MCP (Model Context Protocol) types and interfaces
 */

export interface MCPRequest {
  jsonrpc: "2.0";
  id?: string | number;
  method: string;
  params?: any;
}

export interface MCPResponse {
  jsonrpc: "2.0";
  id?: string | number | undefined | null;
  result?: any;
  error?: {
    code: number;
    message: string;
    data?: any;
  };
}

export interface MCPNotification {
  jsonrpc: "2.0";
  method: string;
  params?: any;
}

export interface Tool {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, any>;
    required?: string[];
  };
}

export interface Resource {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}

export interface CallToolRequest {
  name: string;
  arguments?: any;
}

export interface CallToolResult {
  content: Array<{
    type: "text" | "image" | "resource";
    text?: string;
    data?: string;
    uri?: string;
  }>;
  isError?: boolean;
}

// WebSocket subscription types
export interface JobSubscription {
  jobId: string;
  events: ("logs" | "status" | "completion")[];
  clientId: string;
}

export interface JobNotification {
  type: "job/logs" | "job/status_changed" | "job/completed";
  jobId: string;
  data: any;
}
