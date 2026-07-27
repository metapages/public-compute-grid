# Files in & out

Every value entering or leaving a container is a **DataRef** — a small JSON object saying either "here is the data" or
"here is where to get it".

```ts
type DataRef = {
  value: string; // the data, or a locator
  type?: "utf8" | "base64" | "json" | "url" | "key";
  hash?: string;
};
```

| Type     | `value` holds             | Use for                             |
| -------- | ------------------------- | ----------------------------------- |
| `utf8`   | The text itself (default) | Scripts, config, small text         |
| `json`   | A JSON value              | Structured parameters               |
| `base64` | Base64-encoded bytes      | Small binaries                      |
| `url`    | A URL to fetch            | Anything large; also any public URL |
| `key`    | A blob-storage key        | Internal                            |

The threshold is **200 bytes**. Below it, inline; above it, upload and reference. The clients do this automatically; if
you are writing your own, [do it manually](#uploading-large-inputs).

## Inputs

```json
{
  "definition": {
    "image": "python:3.12-slim",
    "command": "python /inputs/run.py",
    "inputs": {
      "run.py": { "type": "utf8", "value": "print('hi')" },
      "params.json": { "type": "json", "value": { "iterations": 100 } },
      "model.bin": { "type": "url", "value": "https://container.mtfm.io/f/9f86d0…" },
      "public.csv": { "type": "url", "value": "https://example.org/data.csv" }
    }
  }
}
```

Keys are filenames; the worker materialises each one under `/inputs` before starting the container. A `url` ref can
point anywhere the worker can reach, not just at blob storage — handy for public datasets.

`configFiles` works identically but is folded into the job hash, so changing one produces a different job id (and
therefore a real re-run).

### Uploading large inputs {#uploading-large-inputs}

The blob key **is** the sha256 of the content, which makes uploads idempotent and deduplicated across everyone using the
instance.

```js
import { createHash } from "node:crypto";
const API = "https://container.mtfm.io";

const bytes = await readFile("model.bin");
const hash = createHash("sha256").update(bytes).digest("hex");

if ((await fetch(`${API}/f/${hash}/exists`)).status !== 200) {
  await fetch(`${API}/f/${hash}`, { method: "PUT", body: bytes, redirect: "follow" });
}

const ref = { type: "url", value: `${API}/f/${hash}` };
```

```sh
# same thing with curl — --location matters, the API redirects to a signed URL
HASH=$(shasum -a 256 model.bin | cut -d' ' -f1)
curl --location --fail-with-body -X PUT --upload-file model.bin \
  https://container.mtfm.io/f/$HASH
```

`PUT /f/:key` and `GET /f/:key` both redirect to a signed storage URL — always follow redirects.

## Outputs

Whatever the container writes to `/outputs` comes back:

```json
"outputs": {
  "count.txt":   { "type": "base64", "value": "NTE1IC9pbnB1dHMvYmlnLnR4dAo=" },
  "big-out.txt": { "type": "url",    "value": "https://container.mtfm.io/f/bbeb7d46…" }
}
```

Same rule in reverse — small files inline as base64, large ones uploaded and referenced.

Three ways to read one:

```js
// 1. resolve the ref yourself
const bytes = ref.type === "url"
  ? Buffer.from(await (await fetch(ref.value)).arrayBuffer())
  : Buffer.from(ref.value, ref.type === "base64" ? "base64" : "utf8");

// 2. let the API resolve it — works for inline and referenced alike
const bytes2 = Buffer.from(
  await (await fetch(`${API}/q/${queue}/j/${jobId}/outputs/report.pdf`)).arrayBuffer(),
);
```

```sh
# 3. curl, following the redirect
curl -sL https://container.mtfm.io/q/$QUEUE/j/$JOB_ID/outputs/report.pdf -o report.pdf
```

Inputs are readable the same way: `GET /j/:jobId/inputs/:file`.

## The cache directory

`/job-cache` (also `$JOB_CACHE`) persists on the worker **between jobs**. It is not part of the job definition and not
returned in results.

```sh
# download once, reuse on every subsequent job on this worker
python -c "
import os, pathlib
cache = pathlib.Path(os.environ['JOB_CACHE']) / 'weights.pt'
if not cache.exists(): download_to(cache)
"
```

This is the difference between a 4-minute ML job and a 40-second one. Because it is per-worker, treat a cache miss as
normal — always guard with an existence check.

## Retention

Blobs and job records on the public instance expire after roughly a month. Copy out anything you need to keep. Self-host
for different retention, or run a worker in `--mode=local` and nothing is uploaded at all.
