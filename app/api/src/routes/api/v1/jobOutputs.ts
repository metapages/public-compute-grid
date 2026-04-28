import { type DataRef, DataRefType } from "@metapages/compute-queues-shared";
import { db } from "@/db/db.ts";
import { getDownloadPresignedUrl } from "@/routes/api/v1/download.ts";
import type { Context } from "hono";
import mime from "mime";

export const getJobOutputsHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    if (!jobId) {
      c.status(400);
      return c.text("Missing jobId");
    }

    const filename = c.req.path.split("/outputs/").splice(1).join("/inputs/");
    if (!filename) {
      c.status(400);
      return c.text("Missing filename");
    }

    // Get the job definition to find the input file SHA
    const finishedJobFull = await db.getJobFinishedResults(jobId);
    if (!finishedJobFull) {
      c.status(404);
      return c.text("Finished job not found");
    }

    // Look for the file in inputs
    const outputs = finishedJobFull.finished?.result?.outputs;
    if (!outputs || !outputs[filename]) {
      c.status(404);
      return c.text(`Output file '${filename}' not found`);
    }

    const ref: DataRef = outputs[filename];
    switch (ref.type) {
      case DataRefType.url: {
        // if it is a URL then we can redirect to it
        const fId = new URL(ref.value).pathname.split("/")[2];
        if (fId) {
          const url = await getDownloadPresignedUrl(fId);

          if (url) {
            return c.redirect(url, 302);
          }
        }
        return c.redirect(ref.value, 302);
      }
      case DataRefType.base64:
        try {
          // Method 1: Using atob() (browser/modern environments)
          const binaryString = atob(ref.value);
          const bytes = new Uint8Array(binaryString.length);
          for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }
          return new Response(bytes, {
            headers: {
              "Content-Type": mime.getType(filename) || "application/octet-stream",
              "Content-Disposition": `attachment; filename="${filename.split("/").pop()}"`,
              "Content-Length": bytes.length.toString(),
            },
          });
        } catch (error: unknown) {
          console.error("Invalid base64 data", error);
          return c.json({ error: "Invalid base64 data" }, 400);
        }
      case DataRefType.json:
        return c.json(ref.value);
      case DataRefType.utf8:
        return c.text(ref.value);
      case DataRefType.key:
        console.error(`Input file '${filename}' is a key, not supported`);
        return c.json({ error: "Unknown data type" }, 400);
      default:
        return c.json({ error: "Unknown data type" }, 400);
    }
  } catch (err) {
    console.error("Error getting job input file:", err);
    return c.text((err as Error).message, 500);
  }
};
