import { DataRefType } from "@metapages/compute-queues-shared";
import type { Context } from "hono";
import { GetObjectCommand } from "aws-sdk/client-s3";
import { getSignedUrl } from "aws-sdk/s3-request-presigner";

import { bucketParams, s3Client } from "@/routes/s3config.ts";

export const downloadHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");

  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }
  // console.log('params', params);

  // Add headers for
  // https://www.reddit.com/r/aws/comments/j5lhhn/limiting_the_s3_put_file_size_using_presigned_urls/
  // In your service that's generating pre-signed URLs, use the Content-Length header as part of the V4 signature (and accept object size as a parameter from the app). In the client, specify the Content-Length when uploading to S3.
  // Your service can then refuse to provide a pre-signed URL for any object larger than some configured size.
  //  ContentLength: 4
  // ContentMD5?: string;
  // ContentType?: string;
  const command = new GetObjectCommand({ ...bucketParams, Key: key });
  let url = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
  url = url.replace("http://", "https://");
  return c.json({
    url,
    ref: {
      value: url,
      type: DataRefType.url,
    },
  });
};
