import { bucketParams, s3Client } from "@/routes/s3config.ts";
import { GetObjectCommand } from "aws-sdk/client-s3";
import type { Context } from "hono";
import { ms } from "ms";

import { getSignedUrlWithRetry } from "./upload.ts";

const OneWeekInSeconds = (ms("1 week") as number) / 1000;

export const downloadHandler = async (c: Context) => {
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
  try {
    const url = await getDownloadPresignedUrl(key);
    return c.redirect(url);
  } catch (err) {
    console.error("Error downloading file:", err);
    return c.text((err as Error).message, 500);
  }
};

export const getDownloadPresignedUrl = async (key: string): Promise<string> => {
  // Add headers for
  // https://www.reddit.com/r/aws/comments/j5lhhn/limiting_the_s3_put_file_size_using_presigned_urls/
  // In your service that's generating pre-signed URLs, use the Content-Length header as part of the V4 signature (and accept object size as a parameter from the app). In the client, specify the Content-Length when uploading to S3.
  // Your service can then refuse to provide a pre-signed URL for any object larger than some configured size.
  //  ContentLength: 4
  // ContentMD5?: string;
  // ContentType?: string;
  try {
    const command = new GetObjectCommand({ ...bucketParams, Key: key });
    const url = await getSignedUrlWithRetry(s3Client, command, {
      expiresIn: OneWeekInSeconds,
    }, 10);
    return url;
  } catch (err) {
    console.error(`Error getDownloadPresignedUrl key:${key}`, err);
    throw err;
  }
};
