/**
 * HTTP client for communicating with the worker.metapage.io API
 * This is used by the MCP server to interact with the job queue
 */

export interface JobQueueClient {
  baseUrl: string;
}

export class WorkerMetapageClient implements JobQueueClient {
  /** Where this process sends its own HTTP requests. */
  public baseUrl: string;
  /** Origin for URLs shown to a user; equals baseUrl unless overridden. */
  public publicUrl: string;

  constructor(baseUrl: string = "https://container.mtfm.io", publicUrl?: string) {
    this.baseUrl = baseUrl;
    this.publicUrl = publicUrl || baseUrl;
  }

  async submitJob(queue: string, jobDefinition: any): Promise<{ jobId: string }> {
    const response = await fetch(`${this.baseUrl}/q/${queue}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(jobDefinition),
    });

    if (!response.ok) {
      throw new Error(`Failed to submit job: ${response.status} ${response.statusText}`);
    }

    const result = await response.json();
    return { jobId: result.jobId || result.id };
  }

  async getJobStatus(jobId: string): Promise<any> {
    // `.json` suffix: the bare /j/<jobId> path serves the browser page.
    const response = await fetch(`${this.baseUrl}/j/${jobId}.json`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get job status: ${response.status} ${response.statusText}`);
    }

    const result = await response.json();
    // The API returns data wrapped in a 'data' property
    return result.data || result;
  }

  async getJobResult(jobId: string): Promise<any> {
    const response = await fetch(`${this.baseUrl}/j/${jobId}/result.json`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get job result: ${response.status} ${response.statusText}`);
    }

    return await response.json();
  }

  /**
   * The container's own run result — StatusCode, logs, outputs — unwrapped.
   *
   * getJobResult returns the raw `{data: …}` envelope and uses the unqueued
   * route, whose shape differs; reading `.finished.result` off that yields
   * undefined, which is how follow_job was reporting no exit code.
   */
  async getJobRunResult(queue: string, jobId: string): Promise<Record<string, any> | undefined> {
    const response = await fetch(
      `${this.baseUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/result.json`,
      { headers: { "Content-Type": "application/json" } },
    );
    if (!response.ok) {
      throw new Error(`Failed to get job result: ${response.status} ${response.statusText}`);
    }
    const body = await response.json();
    return body?.data?.finished?.result;
  }

  async listJobs(queue: string): Promise<any> {
    const response = await fetch(`${this.baseUrl}/q/${queue}`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to list jobs: ${response.status} ${response.statusText}`);
    }

    return await response.json();
  }

  /**
   * `namespace` matters: submit_job always attaches one (default "dev"), and the
   * API cancels within a single namespace, defaulting to "_". Cancelling
   * without it therefore never matched the job, which kept running and held a
   * worker slot. "*" cancels across every namespace the job is in.
   */
  async cancelJob(queue: string, jobId: string, namespace: string = "*"): Promise<void> {
    const url = `${this.baseUrl}/q/${queue}/j/${jobId}/cancel?namespace=${encodeURIComponent(namespace)}`;
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to cancel job: ${response.status} ${response.statusText}`);
    }
  }

  async uploadFile(key: string, content: string | ArrayBuffer): Promise<string> {
    const response = await fetch(`${this.baseUrl}/f/${key}`, {
      method: "PUT",
      body: content,
    });

    if (!response.ok) {
      throw new Error(`Failed to upload file: ${response.status} ${response.statusText}`);
    }

    return key;
  }

  async downloadFile(key: string): Promise<ArrayBuffer> {
    const response = await fetch(`${this.baseUrl}/f/${key}`, {
      method: "GET",
    });

    if (!response.ok) {
      throw new Error(`Failed to download file: ${response.status} ${response.statusText}`);
    }

    return await response.arrayBuffer();
  }
}

/** One event from the job stream. `kind` mirrors the SSE event name. */
export interface JobStreamEvent {
  kind: "build-log" | "run-log" | "state" | "final";
  /** Log text, one entry per line, for the *-log kinds. */
  lines?: string[];
  state?: string;
  reason?: string;
}

/**
 * Follow a job to completion over the server's SSE endpoint, invoking `onEvent`
 * as things happen. Resolves once the job reaches a terminal state or the
 * signal aborts.
 *
 * This is what makes live logs possible for an MCP client: the transport only
 * needs somewhere to forward these events to (progress notifications), rather
 * than polling and buffering.
 */
export async function followJobStream(
  baseUrl: string,
  queue: string,
  jobId: string,
  onEvent: (event: JobStreamEvent) => void | Promise<void>,
  signal?: AbortSignal,
): Promise<{ state: string; reason?: string }> {
  const url = `${baseUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/stream`;
  const response = await fetch(url, {
    headers: { accept: "text/event-stream" },
    signal,
  });
  if (!response.ok || !response.body) {
    throw new Error(`Could not open the job stream (${response.status}) at ${url}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let final: { state: string; reason?: string } | undefined;

  try {
    while (!final) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      // SSE frames are separated by a blank line.
      let split: number;
      while ((split = buffer.indexOf("\n\n")) !== -1) {
        const frame = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);

        let event = "message";
        const dataLines: string[] = [];
        for (const raw of frame.split("\n")) {
          if (raw.startsWith("event:")) event = raw.slice(6).trim();
          else if (raw.startsWith("data:")) dataLines.push(raw.slice(5).trim());
        }
        if (!dataLines.length) continue;

        let payload: Record<string, unknown>;
        try {
          payload = JSON.parse(dataLines.join("\n"));
        } catch {
          continue;
        }

        if (event === "build-log" || event === "run-log") {
          // A ConsoleLogLine is [text, timestamp, isStderr?].
          const lines = ((payload.lines as unknown[]) || [])
            .map((line) => (Array.isArray(line) ? String(line[0] ?? "") : String(line)))
            .map((line) => (line.endsWith("\n") ? line.slice(0, -1) : line));
          await onEvent({ kind: event, lines });
        } else if (event === "state") {
          await onEvent({ kind: "state", state: payload.state as string, reason: payload.reason as string });
        } else if (event === "final") {
          final = { state: payload.state as string, reason: payload.reason as string };
          await onEvent({ kind: "final", ...final });
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }

  return final ?? { state: "Unknown", reason: "stream ended without a final event" };
}
