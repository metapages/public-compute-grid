import { AwsClient } from "aws4fetch";
import fetchRetry from "fetch-retry";
import { ms } from "ms";

import { getEnv } from "./env.ts";

const fetchWithRetry = fetchRetry(globalThis.fetch);

const OneDayInSeconds = (ms("1 day") as number) / 1000;

export interface BlobStoreConfig {
  /** S3 API endpoint. R2: https://<account>.r2.cloudflarestorage.com. Unset means AWS. */
  endpoint?: string;
  /** R2: "auto" */
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * Blob storage over the S3 API: Cloudflare R2 in production, MinIO locally.
 *
 * Signs requests with aws4fetch rather than the AWS SDK: it is plain fetch +
 * WebCrypto, so the same code runs in Workers, Deno, Node and the browser, and
 * it is what Cloudflare recommends for R2.
 */
export class S3BlobStore {
  private readonly client: AwsClient;
  private readonly config: BlobStoreConfig;

  constructor(config: BlobStoreConfig) {
    this.config = config;
    this.client = new AwsClient({
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
      region: config.region,
      service: "s3",
    });
  }

  /** Path-style for custom endpoints (R2, MinIO), virtual-hosted for AWS. */
  objectUrl(key?: string): URL {
    const path = key === undefined ? "" : key.split("/").map(encodeURIComponent).join("/");
    if (this.config.endpoint) {
      return new URL(`${this.config.endpoint.replace(/\/$/, "")}/${this.config.bucket}/${path}`);
    }
    return new URL(`https://${this.config.bucket}.s3.${this.config.region}.amazonaws.com/${path}`);
  }

  private async presign(method: "GET" | "PUT", key: string, expiresInSeconds: number): Promise<string> {
    const url = this.objectUrl(key);
    url.searchParams.set("X-Amz-Expires", String(Math.floor(expiresInSeconds)));
    const signed = await this.client.sign(url.toString(), { method, aws: { signQuery: true } });
    return signed.url;
  }

  presignGet(key: string, expiresInSeconds = OneDayInSeconds): Promise<string> {
    return this.presign("GET", key, expiresInSeconds);
  }

  presignPut(key: string, expiresInSeconds = OneDayInSeconds): Promise<string> {
    return this.presign("PUT", key, expiresInSeconds);
  }

  async exists(key: string): Promise<boolean> {
    const response = await this.client.fetch(this.objectUrl(key).toString(), { method: "HEAD" });
    await response.body?.cancel();
    if (response.status === 404) {
      return false;
    }
    if (!response.ok) {
      throw new Error(`HEAD ${key} failed: ${response.status}`);
    }
    return true;
  }

  async delete(key: string): Promise<void> {
    const response = await this.client.fetch(this.objectUrl(key).toString(), { method: "DELETE" });
    await response.body?.cancel();
    if (!response.ok && response.status !== 404) {
      throw new Error(`DELETE ${key} failed: ${response.status}`);
    }
  }

  /**
   * CreateBucket, for local stacks only: R2 and S3 buckets are created by
   * `just cloudflare setup` and the console, with credentials that are not
   * allowed to create buckets at all.
   *
   * Idempotent — an existing bucket answers 409 (BucketAlreadyExists, or
   * BucketAlreadyOwnedByYou on AWS), which is success here.
   */
  async ensureBucket(): Promise<void> {
    const response = await this.client.fetch(this.objectUrl().toString(), { method: "PUT" });
    const body = response.ok ? "" : await response.text();
    if (response.ok || response.status === 409) {
      return;
    }
    throw new Error(
      `Could not create bucket ${this.config.bucket} at ${this.config.endpoint ?? "AWS"}: ` +
        `${response.status} ${body.slice(0, 300)}`,
    );
  }

  /** HeadBucket: an R2 token scoped to one bucket may not list buckets. */
  async checkConnection(): Promise<void> {
    const response = await this.client.fetch(this.objectUrl().toString(), { method: "HEAD" });
    await response.body?.cancel();
    if (!response.ok) {
      throw new Error(`HEAD bucket ${this.config.bucket} failed: ${response.status}`);
    }
  }
}

export const blobStoreConfigFromEnv = (): BlobStoreConfig => ({
  endpoint: getEnv("AWS_ENDPOINT") || undefined,
  region: getEnv("AWS_REGION") || "us-west-2",
  bucket: getEnv("AWS_S3_BUCKET") || "metaframe-asman-test",
  accessKeyId: getEnv("AWS_ACCESS_KEY_ID") || "",
  secretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY") || "",
});

let blobStore: S3BlobStore | undefined;

/** The process-wide blob store, configured from the environment on first use. */
export const getBlobStore = (): S3BlobStore => {
  if (!blobStore) {
    const config = blobStoreConfigFromEnv();
    blobStore = new S3BlobStore(config);
    blobStore.checkConnection().catch((err) => {
      console.error(`Blob store connection check failed: ${err}`, {
        bucket: config.bucket,
        region: config.region,
        endpoint: config.endpoint,
      });
    });
  }
  return blobStore;
};

export const putJsonToS3 = async (
  key: string,
  data: unknown,
): Promise<void> => {
  const urlUpload = await getBlobStore().presignPut(key);

  const responseUpload = await fetchWithRetry(urlUpload, {
    method: "PUT",
    redirect: "follow",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json" },
    signal: AbortSignal.timeout(30000),
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

export const deleteFromS3 = async (key: string): Promise<void> => {
  try {
    await getBlobStore().delete(key);
  } catch (err) {
    // swallow errors
    console.log(`Ignored error deleting object ${key} from S3: ${err}`);
  }
};

const getObject = async (key: string): Promise<string | undefined> => {
  const url = await getBlobStore().presignGet(key);

  // Retry logic for handling transient network errors
  const maxRetries = 3;
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        method: "GET",
        redirect: "follow",
        signal: AbortSignal.timeout(30000),
      });

      if (response.status === 404) {
        await response.body?.cancel();
        return undefined;
      }

      if (!response.ok) {
        await response.body?.cancel();
        throw new Error(
          `Failed to fetch from S3: status ${response.status} ${url}`,
        );
      }

      return await response.text();
    } catch (err) {
      lastError = err as Error;

      if (!isRetryableError(err as Error) || attempt === maxRetries) {
        throw err;
      }

      // Exponential backoff with jitter
      const delay = Math.min(
        1000 * Math.pow(2, attempt - 1) + Math.random() * 1000,
        5000,
      );
      console.warn(
        `getObject attempt ${attempt} failed for key ${key}, retrying in ${delay}ms:`,
        err,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError;
};
