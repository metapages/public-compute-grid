/**
 * WebSocket client for connecting to job API endpoints
 * Used by MCP server and other services to receive job updates directly
 */

import type { JobStatusPayload } from "@metapages/compute-queues-shared";

export interface JobWebSocketClient {
  connect(queueName: string): Promise<void>;
  disconnect(): void;
  subscribeToJob(jobId: string): void;
  unsubscribeFromJob(jobId: string): void;
  onJobLogs(callback: (logs: JobStatusPayload) => void): void;
  onJobStatusChange(callback: (jobId: string, status: string, data?: any) => void): void;
  isConnected(): boolean;
}

export class WebSocketJobClient implements JobWebSocketClient {
  private ws: WebSocket | null = null;
  private queueName: string | null = null;
  private subscribedJobs = new Set<string>();
  private logCallbacks: ((logs: JobStatusPayload) => void)[] = [];
  private statusCallbacks: ((jobId: string, status: string, data?: any) => void)[] = [];
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  private baseUrl: string;

  constructor(baseUrl: string = "") {
    // If no base URL provided, use the current origin
    this.baseUrl = baseUrl || (typeof window !== "undefined" ? window.location.origin : "http://localhost:8080");
  }

  async connect(queueName: string): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    this.queueName = queueName;
    const wsUrl = `${this.baseUrl.replace(/^http/, "ws")}/ws/client/${queueName}`;

    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          console.log(`🔌 WebSocket connected to queue: ${queueName}`);
          this.reconnectAttempts = 0;

          // Re-subscribe to any jobs we were previously subscribed to
          for (const jobId of this.subscribedJobs) {
            this.subscribeToJob(jobId);
          }

          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            this.handleMessage(message);
          } catch (error) {
            console.error("Failed to parse WebSocket message:", error);
          }
        };

        this.ws.onclose = () => {
          console.log(`🔌 WebSocket disconnected from queue: ${queueName}`);
          this.attemptReconnect();
        };

        this.ws.onerror = (error) => {
          console.error("WebSocket error:", error);
          reject(error);
        };
      } catch (error) {
        reject(error);
      }
    });
  }

  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.subscribedJobs.clear();
    this.queueName = null;
  }

  subscribeToJob(jobId: string): void {
    this.subscribedJobs.add(jobId);

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      // Send subscription request - this depends on the job API protocol
      // For this implementation, we'll just store the subscription locally
      // and handle incoming messages that match our subscribed jobs
      console.log(`📡 Subscribed to job: ${jobId}`);
    }
  }

  unsubscribeFromJob(jobId: string): void {
    this.subscribedJobs.delete(jobId);
    console.log(`📡 Unsubscribed from job: ${jobId}`);
  }

  onJobLogs(callback: (logs: JobStatusPayload) => void): void {
    this.logCallbacks.push(callback);
  }

  onJobStatusChange(callback: (jobId: string, status: string, data?: any) => void): void {
    this.statusCallbacks.push(callback);
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  private handleMessage(message: any): void {
    try {
      // Handle different message types based on job API protocol
      switch (message.type) {
        case "JobStatusPayload":
          const logs = message.payload as JobStatusPayload;
          if (this.subscribedJobs.has(logs.jobId)) {
            this.logCallbacks.forEach((callback) => {
              try {
                callback(logs);
              } catch (error) {
                console.error("Error in log callback:", error);
              }
            });
          }
          break;

        case "JobStates":
        case "JobStateUpdates":
          const jobStates = message.payload?.state?.jobs || {};
          for (const [jobId, jobState] of Object.entries(jobStates)) {
            if (this.subscribedJobs.has(jobId)) {
              this.statusCallbacks.forEach((callback) => {
                try {
                  callback(jobId, (jobState as any).state, jobState);
                } catch (error) {
                  console.error("Error in status callback:", error);
                }
              });
            }
          }
          break;

        default:
          // Handle other message types as needed
          break;
      }
    } catch (error) {
      console.error("Error handling WebSocket message:", error);
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error(`Max reconnection attempts (${this.maxReconnectAttempts}) reached`);
      return;
    }

    if (!this.queueName) {
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);

    console.log(`Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts}) in ${delay}ms...`);

    setTimeout(() => {
      if (this.queueName) {
        this.connect(this.queueName).catch((error) => {
          console.error("Reconnection failed:", error);
        });
      }
    }, delay);
  }
}
