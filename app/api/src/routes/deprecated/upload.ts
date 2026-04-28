import { DataRefType } from "@metapages/compute-queues-shared";
import type { Context } from "hono";
import { PutObjectCommand } from "aws-sdk/client-s3";
import { getSignedUrl } from "aws-sdk/s3-request-presigner";

import { bucketParams, s3Client } from "@/routes/s3config.ts";

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
    let url = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    url = url.replace("http://", "https://");
    return c.json({
      url,
      ref: {
        value: key, // no http means we know it's an internal address, workers will know how to reach
        type: DataRefType.key,
      },
    });
  } catch (err) {
    console.error("uploadHandler error", err);
    c.status(500);
    return c.text("Failed to get signed URL");
  }
};
