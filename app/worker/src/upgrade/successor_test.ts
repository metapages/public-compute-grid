import type { ContainerInspectInfo, ImageInspectInfo } from "dockerode";
import { assertEquals } from "std/assert";

import { demuxDockerLogs } from "./autoUpdate.ts";
import { ownContainerIdCandidates } from "./self.ts";
import { HANDOVER_FROM_ENV, HANDOVER_ID_ENV, successorImage, successorSpec } from "./successor.ts";

const ID = "a".repeat(64);

Deno.test("successorImage: only the version in the tag moves", () => {
  assertEquals(
    successorImage("metapage/metaframe-docker-worker:0.54.87", "0.54.88", false),
    "metapage/metaframe-docker-worker:0.54.88",
  );
  assertEquals(
    successorImage("metapage/metaframe-docker-worker:0.54.87-standalone", "0.54.88", true),
    "metapage/metaframe-docker-worker:0.54.88-standalone",
  );
  assertEquals(
    successorImage("registry.local:5000/team/worker:0.54.87@sha256:" + "b".repeat(64), "0.54.88", false),
    "registry.local:5000/team/worker:0.54.88",
  );
  assertEquals(
    successorImage("metapage/metaframe-docker-worker", "0.54.88", false),
    "metapage/metaframe-docker-worker:0.54.88",
  );
  assertEquals(
    successorImage("metapage/metaframe-docker-worker:latest", "0.54.88", true),
    "metapage/metaframe-docker-worker:0.54.88-standalone",
  );
  assertEquals(
    successorImage("sha256:" + "c".repeat(64), "0.54.88", false),
    "metapage/metaframe-docker-worker:0.54.88",
  );
});

const image = {
  Config: {
    Env: ["PATH=/usr/bin", "DENO_DIR=/deno", "VERSION=0.54.87"],
    Labels: { "container.mtfm.io": "worker" },
    Entrypoint: ["deno", "run", "--allow-all", "src/cli.ts"],
    Cmd: null,
    WorkingDir: "/app/app/worker",
  },
} as unknown as ImageInspectInfo;

const current = {
  Id: ID,
  Name: "/worker-public1",
  Image: "sha256:" + "d".repeat(64),
  Config: {
    Hostname: ID.slice(0, 12),
    Image: "metapage/metaframe-docker-worker:0.54.87",
    Env: [
      "PATH=/usr/bin",
      "DENO_DIR=/deno",
      "VERSION=0.54.87",
      "METAPAGE_IO_WORKER_RUN_STANDALONE=false",
      "METAPAGE_IO_WORKER_CPUS=4",
      `${HANDOVER_FROM_ENV}=${"e".repeat(64)}`,
      `${HANDOVER_ID_ENV}=old-id`,
    ],
    Labels: { "container.mtfm.io": "worker", "team": "gpu" },
    Entrypoint: ["deno", "run", "--allow-all", "src/cli.ts"],
    Cmd: ["run", "--cpus=4", "--gpus=2", "--max-job-duration=1h", "--api-address=https://example.com", "public1"],
    WorkingDir: "/app/app/worker",
    Tty: true,
    OpenStdin: true,
  },
  HostConfig: {
    Binds: ["/var/run/docker.sock:/var/run/docker.sock", "/tmp:/tmp"],
    NetworkMode: "workers",
    RestartPolicy: { Name: "unless-stopped", MaximumRetryCount: 0 },
    Privileged: true,
    DeviceRequests: [{ Driver: "nvidia", Count: -1, Capabilities: [["gpu"]] }],
  },
  NetworkSettings: {
    Networks: {
      workers: { Aliases: [ID.slice(0, 12), "worker"], IPAMConfig: null },
      monitoring: { Aliases: null },
    },
  },
} as unknown as ContainerInspectInfo;

Deno.test("successorSpec: same config, only the image changed", () => {
  const { options, extraNetworks } = successorSpec({
    current,
    currentImage: image,
    image: "metapage/metaframe-docker-worker:0.54.88",
    name: "worker-public1-0.54.88",
    env: { [HANDOVER_FROM_ENV]: ID, [HANDOVER_ID_ENV]: "the-id" },
  });
  assertEquals(options.Image, "metapage/metaframe-docker-worker:0.54.88");
  assertEquals(options.name, "worker-public1-0.54.88");
  // the args, verbatim
  assertEquals(options.Cmd, current.Config.Cmd);
  // the image's own defaults are left to the new image
  assertEquals(options.Entrypoint, undefined);
  assertEquals(options.WorkingDir, undefined);
  assertEquals(options.Hostname, undefined);
  // the user's env and labels carry over; old handover env is replaced
  assertEquals(options.Env, [
    "METAPAGE_IO_WORKER_RUN_STANDALONE=false",
    "METAPAGE_IO_WORKER_CPUS=4",
    `${HANDOVER_FROM_ENV}=${ID}`,
    `${HANDOVER_ID_ENV}=the-id`,
  ]);
  assertEquals(options.Labels, { "team": "gpu" });
  // mounts, network, restart policy, gpus, privileged
  assertEquals(options.HostConfig, current.HostConfig);
  assertEquals(options.Tty, true);
  assertEquals(options.OpenStdin, true);
  assertEquals(options.NetworkingConfig, { EndpointsConfig: { workers: { Aliases: ["worker"] } } });
  assertEquals(extraNetworks, [{ name: "monitoring", config: {} }]);
});

Deno.test("successorSpec: a chosen hostname and host networking carry over", () => {
  const { options, extraNetworks } = successorSpec({
    current: {
      ...current,
      Config: { ...current.Config, Hostname: "gpu-box" },
      HostConfig: { ...current.HostConfig, NetworkMode: "host" },
      NetworkSettings: { Networks: { host: {} } },
    } as unknown as ContainerInspectInfo,
    currentImage: image,
    image: "x:0.54.88",
    name: "n",
    env: {},
  });
  assertEquals(options.Hostname, "gpu-box");
  assertEquals(options.HostConfig?.NetworkMode, "host");
  assertEquals(options.NetworkingConfig, undefined);
  assertEquals(extraNetworks, []);
});

Deno.test("ownContainerIdCandidates: cgroup v2 mountinfo, cgroup v1, hostname", () => {
  const mountinfo = `1234 1200 0:50 /docker/containers/${ID}/hostname /etc/hostname rw,relatime - ext4 /dev/vda1 rw\n`;
  assertEquals(ownContainerIdCandidates(mountinfo, "0::/\n", "aaaaaaaaaaaa"), [ID, "aaaaaaaaaaaa"]);
  const v1 = `12:pids:/docker/${"f".repeat(64)}\n`;
  assertEquals(ownContainerIdCandidates("", v1, ""), ["f".repeat(64)]);
  assertEquals(ownContainerIdCandidates("", "", ""), []);
});

Deno.test("demuxDockerLogs: framed and raw", () => {
  const frame = (stream: number, text: string) => {
    const payload = new TextEncoder().encode(text);
    const header = new Uint8Array(8);
    header[0] = stream;
    new DataView(header.buffer).setUint32(4, payload.length);
    return [...header, ...payload];
  };
  assertEquals(demuxDockerLogs(new Uint8Array([...frame(1, "out\n"), ...frame(2, "err\n")])), "out\nerr\n");
  assertEquals(demuxDockerLogs(new TextEncoder().encode("raw tty output\n")), "raw tty output\n");
});
