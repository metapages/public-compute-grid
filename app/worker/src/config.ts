import parseDuration from "parse-duration";

export const VERSION: string = "0.2.0";

export interface GpuConfig {
  // Total number of available GPUs (for capacity calculation)
  totalGpus: number;
  // Specific GPU device indices this worker can use (empty array means sequential 0,1,2...)
  deviceIndices: number[];
  // Original GPU specification string for reference
  originalSpec: string;
}

export interface Arguments {
  // This is a proxy for the number of jobs, since currently, jobs
  // are not prevented from claiming as many cpu cpus as they want.
  cpus: number;
  server: string;
  // version?: Boolean;
  queue: string;
  id: string;
  // This is the most GPUs this worker will claim.
  // https://docs.docker.com/engine/containers/resource_constraints/#gpu
  gpus: number;
  // Enhanced GPU configuration parsed from gpus string
  gpuConfig: GpuConfig;
  // Currently "remote" (default) and "local" modes are supported.
  mode: string;
  port: number;
  dataDirectory: string;
  debug: boolean;
  maxJobDuration: number;
  // If true, containers are not deleted after they finish.
  DebugDisableContainerDeletion: boolean;
  // Replace this worker with the latest release when one is out (remote mode).
  autoUpdate: boolean;
  // How often to check for a new release, in ms.
  autoUpdateInterval: number;
  // On startup, kill the running job containers of other worker ids on this
  // docker host (see DockerJobQueue.killOrphanedJobs).
  killOrphans: boolean;
}

/**
 * Global configuration for the worker.
 * Too much effort passing this around in functions, this is truly global
 * configuration, set once at the start of the program.
 */
export const config: Arguments = {
  cpus: 1, //cpus: { type: Number, alias: 'c', description: 'Number of CPUs allowed (default 1)', defaultValue: 1 },
  server: "https://container.mtfm.io", // { type: String, alias: 's', description: `Custom server (default: https://container.mtfm.io)`, optional: true, defaultValue: "https://container.mtfm.io" },
  queue: "", //{ type: String, alias: 'q', description: 'Queue id. Browser links to this queue ' },
  id: "", //{ type: String, alias: 'i', description: `Worker Id (default:${MACHINE_ID})`, defaultValue: MACHINE_ID },
  gpus: 0, //{ type: Number, alias: 'g', description: `Enable "--gpus all" flag if the job requests and the worker supports`, optional: true },
  gpuConfig: {
    totalGpus: 0,
    deviceIndices: [],
    originalSpec: "0",
  },
  mode: "remote", //{ type: String, alias: 'm', description: `Mode (default: remote)`, optional: true },
  port: 8000, //{ type: Number, alias: 'p', description: `Port (default: 8000)`, optional: true },
  dataDirectory: "/tmp/worker-metapage-io", //{ type: String, alias: 'd', description: `Data directory (default: /tmp/worker-metapage-io)`, optional: true },
  debug: false, //{ type: Boolean, alias: 'd', description: `Debug mode (default: false)`, optional: true },
  maxJobDuration: parseDuration("5m") as number, //{ type: number, alias: 't', description: `Maximum duration of a job (default: 5m)`, optional: true },
  DebugDisableContainerDeletion: false, // for debugging and testing.
  autoUpdate: false,
  autoUpdateInterval: parseDuration("1h") as number,
  killOrphans: true,
};

export const getConfig = (): Arguments => {
  return config;
};

/**
 * Parse GPU specification string into GpuConfig
 * Supports:
 * - Numbers: "2" -> { totalGpus: 2, deviceIndices: [], originalSpec: "2" }
 * - Device specs: '"device=1,3"' -> { totalGpus: 2, deviceIndices: [1, 3], originalSpec: '"device=1,3"' }
 * - Single device: '"device=1"' -> { totalGpus: 1, deviceIndices: [1], originalSpec: '"device=1"' }
 */
export const parseGpuSpec = (gpuSpec: string | number): GpuConfig => {
  const originalSpec = String(gpuSpec);

  // Handle numeric input (backwards compatibility)
  if (typeof gpuSpec === "number" || /^\d+$/.test(originalSpec)) {
    const count = typeof gpuSpec === "number" ? gpuSpec : parseInt(originalSpec, 10);
    return {
      totalGpus: count,
      deviceIndices: [], // Empty means use sequential allocation (0, 1, 2, ...)
      originalSpec,
    };
  }

  // Handle device specification like '"device=1,3"' or '"device=1"'
  const deviceMatch = originalSpec.match(/['"]*device=([0-9,]+)['"]*$/);
  if (deviceMatch) {
    const deviceIndices = deviceMatch[1].split(",").map((d) => parseInt(d.trim(), 10));
    return {
      totalGpus: deviceIndices.length,
      deviceIndices,
      originalSpec,
    };
  }

  // Handle other Docker GPU specs like "all"
  if (originalSpec === "all" || originalSpec === '"all"') {
    // For "all", we can't determine the exact count without querying the system
    // This would require nvidia-smi or similar, for now return a high number
    return {
      totalGpus: 8, // Reasonable default for "all"
      deviceIndices: [],
      originalSpec,
    };
  }

  // Default fallback
  return {
    totalGpus: 0,
    deviceIndices: [],
    originalSpec,
  };
};
