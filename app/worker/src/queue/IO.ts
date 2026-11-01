import { getConfig } from "/@/config.ts";
import klaw from "klaw";
import { emptyDir, ensureDir, exists, existsSync } from "std/fs";
import { dirname, join } from "std/path";

import {
  type DataRef,
  dataRefToFile,
  DataRefType,
  type DockerJobDefinitionInputRefs,
  fileToDataref,
  getJobColorizedString,
  getWorkerColorizedString,
  hashFileOnDisk,
  type InputsRefs,
  sanitizeFilename,
} from "@metapages/compute-queues-shared";

import type { Volume, WorkdirFile } from "./types.ts";

/**
 * File names come from the job definition, and are joined onto worker paths.
 */
const assertSafeFileName = (name: string) => {
  if (name.split("/").includes("..")) {
    throw new Error(`Invalid file name, ".." is not allowed: ${name}`);
  }
};

/**
 * @param job Returns input and output docker volumes to mount into the container,
 * and the job files relative to the working directory, which is only known once
 * the image is ready.
 */
export const convertIOToVolumeMounts = async (
  job: { id: string; definition: DockerJobDefinitionInputRefs },
  address: string,
): Promise<{ volumes: Volume[]; outputsDir: string; workdirFiles: WorkdirFile[] }> => {
  const config = getConfig();
  const { id, definition } = job;
  const baseDir = join(config.dataDirectory, "j", id);
  const cacheDir = join(config.dataDirectory, "f");
  const filesDir = join(baseDir, "files");
  const inputsDir = join(baseDir, "inputs");
  const outputsDir = join(baseDir, "outputs");

  // create the tmp directory for inputs+outputs
  await ensureDir(cacheDir);
  await ensureDir(filesDir);
  await ensureDir(inputsDir);
  await ensureDir(outputsDir);

  // security/consistency: empty directories, in case restarting jobs
  await emptyDir(filesDir);
  await emptyDir(inputsDir);
  // await emptyDir(outputsDir);

  // make sure directories are writable
  await Deno.chmod(baseDir, 0o777);
  await Deno.chmod(filesDir, 0o777);
  await Deno.chmod(inputsDir, 0o777);
  await Deno.chmod(outputsDir, 0o777);

  // console.log(
  //   `[${workerId.substring(0, 6)}] [${
  //     id.substring(0, 6)
  //   }] creating\n\t ${inputsDir}\n\t ${outputsDir}`,
  // );

  // copy the inputs (if any)
  const inputs = definition.inputs;

  if (inputs) {
    for (const [name, ref] of Object.entries(inputs)) {
      assertSafeFileName(name);
      await dataRefToFile(
        ref,
        join(inputsDir, name),
        address,
        config.dataDirectory,
      );
    }
  }
  const result: Volume[] = [
    {
      host: inputsDir,
      // TODO: allow this to be configurable
      container: "/inputs",
    },
    {
      host: outputsDir,
      // TODO: allow this to be configurable
      container: "/outputs",
    },
  ];

  const workdirFiles: WorkdirFile[] = [];
  if (definition?.files) {
    for (const [name, ref] of Object.entries(definition.files)) {
      assertSafeFileName(name);
      const isAbsolutePath = name.startsWith("/");
      // separate folders so "/app/a" and "app/a" are different files
      const host = join(filesDir, isAbsolutePath ? "absolute" : "relative", name);
      await dataRefToFile(ref, host, address, config.dataDirectory);
      if (isAbsolutePath) {
        result.push({ host, container: name });
      } else {
        workdirFiles.push({ host, name });
      }
    }
  }

  return { volumes: result, outputsDir, workdirFiles };
};

const getLocalDataRef = async (file: string, address: string) => {
  const hash = await hashFileOnDisk(file);
  const sanitizedHash = sanitizeFilename(hash);
  const config = getConfig();
  const cachedFilePath = join(config.dataDirectory, "f", sanitizedHash);
  await ensureDir(dirname(cachedFilePath));

  try {
    if (existsSync(cachedFilePath)) {
      Deno.removeSync(file);
      Deno.linkSync(cachedFilePath, file);
    } else {
      Deno.linkSync(file, cachedFilePath);
    }
  } catch (err) {
    console.error("💥 Failed to link file", file, err);
  }

  const dataRef: DataRef = {
    value: `${address}/f/${sanitizedHash}`,
    type: DataRefType.url,
    hash,
  };
  return dataRef;
};

/**
 * Converts file outputs to datarefs (small JSON references to files in the cloud)
 * @param job
 * @param workerId
 * @returns
 */
export const getOutputs = async (
  jobId: string,
  workerId: string,
): Promise<InputsRefs> => {
  // TODO: duplicate code
  const config = getConfig();
  const baseDir = join(config.dataDirectory, "j", sanitizeFilename(jobId));
  const outputsDir = join(baseDir, "outputs");

  // copy the inputs (if any)
  const outputs: InputsRefs = {};

  const files = await getFiles(outputsDir);

  for (const file of files) {
    // This will send big blobs to the cloud unless local mode
    const ref =
      await (config.mode === "local" ? getLocalDataRef(file, config.server) : fileToDataref(file, config.server));
    outputs[file.replace(`${outputsDir}/`, "")] = ref;
  }

  console.log(
    `${getWorkerColorizedString(workerId)} ${getJobColorizedString(jobId)} outputs:[${
      Object.keys(outputs).join(",").substring(
        0,
        100,
      )
    }]`,
  );
  return outputs;
};

const getFiles = async (path: string): Promise<string[]> => {
  const pathExists = await exists(path);
  if (!pathExists) {
    throw `getFiles path=${path} does not exist`;
  }
  return new Promise((resolve, reject) => {
    const files: string[] = []; // files, full path
    klaw(path)
      // .pipe(excludeDirFilter)
      .on(
        "data",
        (item: { stats: { isDirectory: () => boolean }; path: string }) => {
          if (item && !item.stats.isDirectory()) files.push(item.path);
        },
      )
      .on("error", (err: unknown, item: unknown) => {
        console.error(`error on item`, item);
        console.error(err);
        reject(err);
      })
      .on("end", () => resolve(files));
  });
};
