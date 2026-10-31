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

`inputs` sit next to the `definition`, not inside it: they are what one run of the job processes, and are not part of
the job's id. See [Jobs and runs](#jobs-and-runs).

## Job files

`definition.files` are the job's own scripts and config. They are part of the job's id, so changing one is a different
job.

```json
{
  "definition": {
    "image": "python:3.12-slim",
    "command": "python run.py",
    "files": {
      "run.py": { "type": "utf8", "value": "print('hi')" },
      "conf/settings.json": { "type": "json", "value": { "iterations": 100 } },
      "/etc/myapp/config.toml": { "type": "utf8", "value": "debug = true" }
    }
  }
}
```

A relative path is under the working directory: `definition.workdir`, else the image's `WORKDIR`, else `/`. An absolute
path is used as it is. `..` is not allowed in a path.

`definition.inputs` and `definition.configFiles` are deprecated. They still work, still land in `/inputs` and are part of
the job's id. Use `files` in a definition, and `inputs` next to it.

## Jobs and runs {#jobs-and-runs}

A **job** is its definition. A **run** is a job plus the inputs it processes, so one job can process many inputs.

```js
const { jobId, runId, inputsHash, path } = await (await fetch(`${API}/q/${queue}`, {
  method: "POST",
  body: JSON.stringify({ definition, inputs: { "data.csv": { type: "utf8", value: "a,b" } } }),
})).json();

// run the same job on other data: its id is enough
await fetch(`${API}/q/${queue}`, {
  method: "POST",
  body: JSON.stringify({ jobId, inputs: { "data.csv": { type: "utf8", value: "c,d" } } }),
});
```

| Field        | What it is                                                                    |
| ------------ | ----------------------------------------------------------------------------- |
| `jobId`      | The job: the sha256 of its definition, or a name you chose (see below)        |
| `jobHash`    | The sha256 of the definition: one version of the job                          |
| `inputsHash` | The sha256 of the inputs. Absent with no inputs.                              |
| `runId`      | The run. With no inputs it is the `jobHash`. Poll this, not `jobId`.          |
| `path`       | The canonical URL path of this run. Opens the browser client at that address. |

Every job route takes any of these forms, as does the browser client:

```
/j/<sha256 job>                                the job
/j/<sha256 job>?i=<sha256 inputs>              the job on those inputs
/j/<name>                                      the latest version of a named job
/j/<name>?i=<sha256 inputs>                    its latest version, on those inputs
/j/<name>?v=<sha256 job>&i=<sha256 inputs>     a pinned version, on those inputs
/j/<job>#?i=<json>                             browser only: inputs in the URL, not yet run
/j/<runId>                                     a run, directly
```

`?i=` and `?v=` work the same on the sub-paths, e.g. `/q/<queue>/j/<job>/result.json?i=<sha256 inputs>`. A combination
that was never submitted answers `404`.

`#?i=` is `base64(encodeURIComponent(json))`, the same as `#?job=`, of `{name: text or DataRef}`. If `#?i=` or `#?files=` does not hold such a JSON object, it stays an ordinary hash param that reaches the job as an env var, as in `#?i=3`. Large values have to be
DataRefs to fit in a URL.

### Named jobs

Submit with your own `jobId`, for example a uuid, to get a short id that stays the same as the job changes. A name is 1-128 characters from `A-Z a-z 0-9 _ -`, and cannot be shaped like a sha256: a sha256 only ever means that content, so a `jobId` that is one has to be the hash of the definition. Anything else is a `400`.

```js
await fetch(`${API}/q/${queue}`, {
  method: "POST",
  body: JSON.stringify({ jobId: crypto.randomUUID(), definition }),
});
```

Submitting a changed definition under the same `jobId` publishes a new version. Results are keyed by content, so the new
version runs, and the old one stays reachable with `?v=`. As with queues, anyone who knows the id can publish to it.

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

Inputs and job files are readable the same way: `GET /j/:jobId/inputs/:file`, `GET /j/:jobId/files/:path`.

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
