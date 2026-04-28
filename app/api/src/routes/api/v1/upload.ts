import { bucketParams, s3Client } from "@/routes/s3config.ts";
import { PutObjectCommand } from "aws-sdk/client-s3";
import { getSignedUrl } from "aws-sdk/s3-request-presigner";
import type { Context } from "hono";
import { ms } from "ms";

const OneWeekInSeconds = (ms("1 week") as number) / 1000;

export const uploadHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");

  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }
  // Add headers for
  // https://www.reddit.com/r/aws/comments/j5lhhn/limiting_the_s3_put_file_size_using_presigned_urls/
  // In your service that's generating pre-signed URLs, use the Content-Length header as part of the V4 signature (and accept object size as a parameter from the app). In the client, specify the Content-Length when uploading to S3.
  // Your service can then refuse to provide a pre-signed URL for any object larger than some configured size.
  //  ContentLength: 4
  // ContentMD5?: string;
  // ContentType?: string;

  const command = new PutObjectCommand({ ...bucketParams, Key: key });
  try {
    const url = await getSignedUrlWithRetry(s3Client, command, {
      expiresIn: OneWeekInSeconds,
    }, 10);
    return c.redirect(url);
  } catch (err) {
    console.error("uploadHandler error", err);
    c.status(500);
    return c.text("Failed to get signed URL");
  }
};

export async function getSignedUrlWithRetry(
  client: typeof s3Client,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- AWS SDK v3 command type is incompatible with getSignedUrl parameter type
  cmd: PutObjectCommand,
  options: Parameters<typeof getSignedUrl>[2],
  maxRetries = 3,
): Promise<string> {
  let delay = 1000;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await getSignedUrl(client, cmd, options);
    } catch (error) {
      // Only retry on 429 errors
      if (!error?.toString().includes("429") || attempt === maxRetries - 1) {
        throw error;
      }

      // Wait with exponential backoff + jitter
      await new Promise((resolve) => setTimeout(resolve, delay + Math.random() * 1000));
      delay *= 2;
    }
  }

  throw new Error("Max retries exceeded for getSignedUrl");
}
