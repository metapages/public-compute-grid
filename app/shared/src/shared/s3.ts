import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListBucketsCommand,
  PutObjectCommand,
  S3Client,
} from "aws-sdk/client-s3";
import { getSignedUrl } from "aws-sdk/s3-request-presigner";
import fetchRetry from "fetch-retry";
import { ms } from "ms";

const fetch = fetchRetry(globalThis.fetch);

// import { S3Client } from 'https://deno.land/x/s3_lite_client@0.7.0/mod.ts';
const OneDayInSeconds = (ms("1 day") as number) / 1000;

const Bucket: string = Deno.env.get("AWS_S3_BUCKET") || "metaframe-asman-test";
const AWS_REGION: string = Deno.env.get("AWS_REGION") || "us-west-2";
const AWS_ENDPOINT: string | undefined = Deno.env.get("AWS_ENDPOINT");
const AWS_ACCESS_KEY_ID: string = Deno.env.get("AWS_ACCESS_KEY_ID")!;
const AWS_SECRET_ACCESS_KEY: string = Deno.env.get("AWS_SECRET_ACCESS_KEY")!;

export const bucketParams = {
  Bucket,
  ContentType: "application/octet-stream",
};

const config = {
  sslEnabled: AWS_ACCESS_KEY_ID?.includes("minio") ? false : true,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
  region: AWS_REGION,
  endpoint: AWS_ENDPOINT,
  forcePathStyle: AWS_ENDPOINT ? true : undefined,
  signatureVersion: "v4",
  // Add connection timeout settings for better stability
  requestHandler: {
    // Increase timeout for S3 operations
    requestTimeout: 30000, // 30 seconds
  },
  // Add retry configuration for the AWS SDK
  maxAttempts: 3,
  retryMode: "adaptive" as const,
};

export const s3Client = new S3Client(config);
try {
  // ListBuckets takes no bucket-scoped input; this is just a connectivity check.
  const _data = await s3Client.send(new ListBucketsCommand({}));
  // console.log(
  //   "ListBucketsCommand Buckets:",
  //   _data?.Buckets?.map((b) => b.Name),
  // );
  // console.log("S3 Configuration:", {
  //   bucket: Bucket,
  //   region: AWS_REGION,
  //   endpoint: AWS_ENDPOINT,
  //   sslEnabled: config.sslEnabled,
  //   forcePathStyle: config.forcePathStyle,
  //   maxAttempts: config.maxAttempts,
  //   retryMode: config.retryMode,
  // });
} catch (err) {
  console.error(`Failed to ListBucketsCommand: ${err}`);
  console.error("S3 Configuration (failed):", {
    bucket: Bucket,
    region: AWS_REGION,
    endpoint: AWS_ENDPOINT,
    sslEnabled: config.sslEnabled,
    forcePathStyle: config.forcePathStyle,
    maxAttempts: config.maxAttempts,
    retryMode: config.retryMode,
  });
}

export const putJsonToS3 = async (
  key: string,
  data: unknown,
): Promise<void> => {
  const command = new PutObjectCommand({
    ...bucketParams,
    Key: key,
    ContentType: "application/json",
  });

  const urlUpload = await getSignedUrl(s3Client, command, {
    expiresIn: OneDayInSeconds,
  });

  const responseUpload = await fetch(urlUpload, {
    method: "PUT",
    redirect: "follow",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json" },
    // Add timeout to prevent hanging requests
    signal: AbortSignal.timeout(30000), // 30 second timeout
  });

  if (!responseUpload.ok) {
    throw new Error(
      `Failed to upload to S3: status ${responseUpload.status} ${urlUpload}`,
    );
  }

  await responseUpload.text();
};

// Helper function to determine if an error is retryable
const isRetryableError = (error: Error): boolean => {
  const errorMessage = error.message.toLowerCase();
  const errorString = error.toString().toLowerCase();

  // Retry on network-related errors
  const retryablePatterns = [
    "connection error",
    "peer closed connection",
    "tls close_notify",
    "unexpected eof",
    "network error",
    "timeout",
    "connection reset",
    "connection refused",
    "host unreachable",
    "network unreachable",
    "temporary failure",
    "service unavailable",
    "internal server error",
    "bad gateway",
    "gateway timeout",
    "connection closed before message completed",
  ];

  return retryablePatterns.some((pattern) => errorMessage.includes(pattern) || errorString.includes(pattern));
};

export const resolveDataRefFromS3 = async <T>(
  key: string,
): Promise<T | undefined> => {
  return await getJsonFromS3(key);
};

export const getJsonFromS3 = async <T>(key: string): Promise<T | undefined> => {
  try {
    const result = await getObject(key);
    if (!result) {
      // console.warn(`getJsonFromS3: No data returned for key ${key}`);
      return undefined;
    }
    return JSON.parse(result) as T;
  } catch (err) {
    const error = err as Error;
    console.error(`getJsonFromS3 error for key ${key}:`, {
      message: error.message,
      stack: error.stack,
      name: error.name,
      key: JSON.stringify(key),
    });

    // Re-throw the error to allow callers to handle it appropriately
    throw error;
  }
};

export const deleteFromS3 = (Key: string): Promise<void> => {
  return new Promise((resolve /* , reject */) => {
    const deleteObjectCommand = new DeleteObjectCommand({
      ...bucketParams,
      Key,
    });

    s3Client
      .send(deleteObjectCommand)
      .then(() => {
        resolve();
      })
      .catch((err: unknown) => {
        console.log(`Ignored error deleting object ${Key} from S3: ${err}`);
        resolve();
        // swallow errors
      });
  });
};

const getObject = async (Key: string): Promise<string | undefined> => {
  const command = new GetObjectCommand({ ...bucketParams, Key });

  const url = await getSignedUrl(s3Client, command, {
    expiresIn: OneDayInSeconds,
  });

  // Retry logic for handling transient network errors
  const maxRetries = 3;
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        // @ts-ignore: TS2353
        method: "GET",
        // @ts-ignore: TS2353
        redirect: "follow",
        // Add timeout to prevent hanging requests
        signal: AbortSignal.timeout(30000), // 30 second timeout
      });

      if (response.status === 404) {
        return undefined;
      }

      if (!response.ok) {
        throw new Error(
          `Failed to fetch from S3: status ${response.status} ${url}`,
        );
      }

      const text = await response.text();
      return text;
    } catch (err) {
      lastError = err as Error;

      // Check if this is a retryable error
      const isRetryable = isRetryableError(err as Error);

      if (!isRetryable || attempt === maxRetries) {
        // console.error(
        //   `getObject failed for key ${Key} after ${attempt} attempts:`,
        //   err,
        // );
        throw err;
      }

      // Exponential backoff with jitter
      const delay = Math.min(
        1000 * Math.pow(2, attempt - 1) + Math.random() * 1000,
        5000,
      );
      console.warn(
        `getObject attempt ${attempt} failed for key ${Key}, retrying in ${delay}ms:`,
        err,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError;
};
