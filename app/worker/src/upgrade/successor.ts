import type { ContainerCreateOptions, ContainerInspectInfo, ImageInspectInfo } from "dockerode";

/**
 * Set on a successor container: the id of the container it replaces. The
 * successor waits in standby (not claiming jobs) until that container is ready
 * to be stopped, stops it, and takes its name.
 */
export const HANDOVER_FROM_ENV = "METAPAGE_IO_WORKER_HANDOVER_FROM";
/**
 * The worker id the successor must use, so the queue sees one worker before
 * and after the upgrade even when the data directory (where the id is
 * persisted) is not shared between the two containers.
 */
export const HANDOVER_ID_ENV = "METAPAGE_IO_WORKER_HANDOVER_ID";

const HANDOVER_ENV_KEYS = [HANDOVER_FROM_ENV, HANDOVER_ID_ENV];

export const DEFAULT_WORKER_IMAGE_REPOSITORY = "metapage/metaframe-docker-worker";

/**
 * The image the successor runs: the same repository, the tag moved to the new
 * version, keeping the tag's suffix (`0.54.87-standalone` becomes
 * `0.54.88-standalone`). A digest pin is dropped, it cannot name another
 * version. A tag that is not a version (`latest`, none) becomes the version,
 * with `-standalone` when the worker runs standalone.
 */
export const successorImage = (image: string, toVersion: string, standalone: boolean): string => {
  const withoutDigest = image.split("@")[0];
  if (/^(sha256:)?[0-9a-f]{12,64}$/.test(withoutDigest)) {
    // started from an image id: nothing to swap a tag on
    return `${DEFAULT_WORKER_IMAGE_REPOSITORY}:${toVersion}${standalone ? "-standalone" : ""}`;
  }
  const lastSlash = withoutDigest.lastIndexOf("/");
  const colon = withoutDigest.lastIndexOf(":");
  const hasTag = colon > lastSlash;
  const repository = hasTag ? withoutDigest.slice(0, colon) : withoutDigest;
  const tag = hasTag ? withoutDigest.slice(colon + 1) : "";
  const versioned = /^v?\d+\.\d+\.\d+(.*)$/.exec(tag);
  const suffix = versioned ? versioned[1] : (standalone ? "-standalone" : "");
  return `${repository}:${toVersion}${suffix}`;
};

const sameJson = (a: unknown, b: unknown) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);

const envKey = (entry: string) => entry.split("=")[0];

export interface SuccessorSpec {
  /** For docker.createContainer. */
  options: ContainerCreateOptions;
  /** Networks beyond the first, to connect after creating (the create API takes one). */
  extraNetworks: { name: string; config: Record<string, unknown> }[];
}

/**
 * Create options for a container that is `current` with only the image
 * changed: the same args, env, mounts, network, restart policy, GPU device
 * requests, privileged flag and labels.
 *
 * `docker inspect` reports the container's config merged with its image's
 * defaults (ENV, LABEL, ENTRYPOINT, WORKDIR...). Copying those verbatim would
 * pin the *old* image's defaults onto the new one, so anything identical to
 * `currentImage`'s default is left out and the new image supplies its own.
 */
export const successorSpec = (args: {
  current: ContainerInspectInfo;
  currentImage?: ImageInspectInfo;
  image: string;
  name: string;
  env: Record<string, string>;
}): SuccessorSpec => {
  const { current, currentImage, image, name, env } = args;
  const config = current.Config;
  const imageConfig = currentImage?.Config;

  const imageEnv = new Set(imageConfig?.Env ?? []);
  const envOut = (config.Env ?? [])
    .filter((entry) => !imageEnv.has(entry))
    .filter((entry) => !HANDOVER_ENV_KEYS.includes(envKey(entry)))
    .filter((entry) => !(envKey(entry) in env));
  for (const [key, value] of Object.entries(env)) {
    envOut.push(`${key}=${value}`);
  }

  const imageLabels = imageConfig?.Labels ?? {};
  const labels = Object.fromEntries(
    Object.entries(config.Labels ?? {}).filter(([key, value]) => imageLabels[key] !== value),
  );

  const options: ContainerCreateOptions = {
    name,
    Image: image,
    Env: envOut,
    Labels: labels,
    Tty: config.Tty,
    OpenStdin: config.OpenStdin,
    StdinOnce: config.StdinOnce,
    AttachStdin: config.AttachStdin,
    AttachStdout: config.AttachStdout,
    AttachStderr: config.AttachStderr,
    HostConfig: { ...current.HostConfig } as ContainerCreateOptions["HostConfig"],
  };

  // The worker's own args: `run --cpus ... <queue>`
  if (!sameJson(config.Cmd, imageConfig?.Cmd)) {
    options.Cmd = config.Cmd as string[];
  }
  if (!sameJson(config.Entrypoint, imageConfig?.Entrypoint)) {
    options.Entrypoint = config.Entrypoint as string | string[];
  }
  if (config.WorkingDir && config.WorkingDir !== imageConfig?.WorkingDir) {
    options.WorkingDir = config.WorkingDir;
  }
  if (config.User && config.User !== imageConfig?.User) {
    options.User = config.User;
  }
  // docker defaults the hostname to the short container id: only a chosen one carries over
  if (config.Hostname && !current.Id.startsWith(config.Hostname)) {
    options.Hostname = config.Hostname;
  }
  if (config.Domainname) {
    options.Domainname = config.Domainname;
  }
  const extra = config as unknown as Record<string, unknown>;
  const imageExtra = (imageConfig ?? {}) as unknown as Record<string, unknown>;
  for (const key of ["StopSignal", "StopTimeout", "Healthcheck"]) {
    if (extra[key] !== undefined && !sameJson(extra[key], imageExtra[key])) {
      (options as Record<string, unknown>)[key] = extra[key];
    }
  }
  const imagePorts = (imageConfig?.ExposedPorts ?? {}) as Record<string, unknown>;
  const ports = Object.fromEntries(
    Object.entries(config.ExposedPorts ?? {}).filter(([port]) => !(port in imagePorts)),
  );
  if (Object.keys(ports).length > 0) {
    options.ExposedPorts = ports;
  }

  // Networks: the create call takes one endpoint, the rest are connected after.
  const extraNetworks: SuccessorSpec["extraNetworks"] = [];
  const networkMode = current.HostConfig?.NetworkMode ?? "default";
  const networks = current.NetworkSettings?.Networks ?? {};
  const shortId = current.Id.slice(0, 12);
  const endpoint = (settings: Record<string, unknown>) => {
    const aliases = ((settings.Aliases as string[] | null) ?? []).filter((alias) =>
      alias !== shortId && alias !== current.Id
    );
    const out: Record<string, unknown> = {};
    if (aliases.length > 0) out.Aliases = aliases;
    if (settings.IPAMConfig) out.IPAMConfig = settings.IPAMConfig;
    if (settings.Links) out.Links = settings.Links;
    if (settings.DriverOpts) out.DriverOpts = settings.DriverOpts;
    return out;
  };
  const isSharedNamespace = networkMode === "host" || networkMode === "none" ||
    networkMode.startsWith("container:");
  if (!isSharedNamespace) {
    const primary = networkMode === "default" ? "bridge" : networkMode;
    for (const [networkName, settings] of Object.entries(networks)) {
      const config = endpoint(settings as unknown as Record<string, unknown>);
      if (networkName === primary) {
        options.NetworkingConfig = { EndpointsConfig: { [networkName]: config } };
      } else {
        extraNetworks.push({ name: networkName, config });
      }
    }
  }

  return { options, extraNetworks };
};
