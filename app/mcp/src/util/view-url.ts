import type { DockerJobDefinitionInputRefs } from "@metapages/compute-queues-shared";

/**
 * Beyond this the link is more noise than help: it bloats every tool response
 * it appears in and browsers start to struggle with the hash.
 */
const MAX_VIEW_URL_LENGTH = 32768;

/**
 * A shareable link that opens the job in the browser client — logs, outputs,
 * and an editable copy of the definition.
 *
 * There is no `/j/<jobId>` viewer route; that path serves JSON. The client
 * reads the whole definition out of the URL hash and re-derives the job id from
 * it, so the definition has to travel in the link:
 *
 *   <baseUrl>/#?job=<base64(encodeURIComponent(json))>&queue=<queue>
 *
 * The id the client derives is the same one we submitted: the only fields it
 * adds on top of the hash param are an empty `env` and `configFiles`, and the
 * job hash blob drops empty records. This is the shape `cq run` prints as
 * `view <url>`.
 *
 * Returns undefined when the definition is too big to embed — the caller should
 * fall back to the REST result URL rather than emit an unusable link.
 */
export const buildViewUrl = (
  baseUrl: string,
  definition: DockerJobDefinitionInputRefs,
  queue: string,
): string | undefined => {
  const encoded = btoa(encodeURIComponent(JSON.stringify(definition)));
  const url = `${baseUrl.replace(/\/$/, "")}/#?job=${encoded}&queue=${encodeURIComponent(queue)}`;
  return url.length > MAX_VIEW_URL_LENGTH ? undefined : url;
};

/**
 * Short, paste-friendly view URL: the job's sha256 id in the path instead of the
 * whole definition in the hash. ~100 characters, so it survives a terminal.
 *
 *   <baseUrl>/j/<jobId>#?queue=<queue>
 *
 * The SPA is served at that path and loads the definition by id from
 * `GET /j/<jobId>/definition.json`; the queue still rides in the hash because a
 * worker only runs a job on the queue it subscribed to. Editing in the page
 * reverts to the self-contained embedded form above.
 *
 * Only valid once the definition is persisted server-side — i.e. after submit.
 * To link to a job that was never submitted, use buildViewUrl.
 */
export const buildShortViewUrl = (
  baseUrl: string,
  jobId: string,
  queue: string,
): string => `${baseUrl.replace(/\/$/, "")}/j/${jobId}#?queue=${encodeURIComponent(queue)}`;
