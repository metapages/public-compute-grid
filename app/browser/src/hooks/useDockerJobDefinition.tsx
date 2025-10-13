/**
 * Via Context provide the current docker job definition which is combined from metaframe inputs
 * and URL query parameters, and the means to change (some of) them
 */
import { useEffect } from "react";

import {
  copyLargeBlobsToCloud,
  DataRefType,
  DefaultNamespace,
  DockerJobControlConfig,
  DockerJobDefinitionInputRefs,
  DockerJobDefinitionMetadata,
  DockerJobDefinitionParamsInUrlHash,
  isDataRef,
  JobInputs,
  shaDockerJob,
} from "/@shared/client";

import { getHashParamsFromWindow } from "@metapages/hash-query";
import { useHashParam, useHashParamBoolean, useHashParamJson } from "@metapages/hash-query/react-hooks";
import { DataRefSerialized, Metaframe } from "@metapages/metapage";
import { useMetaframeAndInput } from "@metapages/metapage-react";

import { getIOBaseUrl } from "../config";
import { useStore } from "../store";
import { useOptionAllowSetJob } from "./useOptionAllowSetJob";
import { useQueue } from "./useQueue";

// make sure these are all in
// app/browser/public/metaframe.json
const HashParamKeysSystem = new Set([
  "allowsetjob",
  "autostart",
  "config",
  "control",
  "debug",
  "definition",
  "ignoreQueueOverride",
  "inputs",
  "job",
  "maxJobDuration",
  "queue",
  "queueOverride",
  "resolverefs",
  "terminal",
]);

const getNonSystemHashParams = (): Record<string, string> => {
  const [_, hashParams] = getHashParamsFromWindow();
  // never inject system params.
  HashParamKeysSystem.forEach(key => {
    delete hashParams[key];
  });
  return hashParams;
};

/**
 * Gets the configuration from 1) the URL hash parameters and 2) the metaframe inputs,
 * combines them together, and sets the docker job definition in the store
 */
export const useDockerJobDefinition = () => {
  const { resolvedQueue } = useQueue();

  // TODO: unclear if this does anything anymore
  const [debug] = useHashParamBoolean("debug");
  const [maxJobDuration] = useHashParam("maxJobDuration");
  // we listen to the job parameters embedded in the URL changing
  const [definitionParamsInUrl, setDefinitionParamsInUrl] = useHashParamJson<
    DockerJobDefinitionParamsInUrlHash | undefined
  >("job");

  // input text files are stored in the URL hash
  const [jobInputsFromUrl] = useHashParamJson<JobInputs | undefined>("inputs");

  // Parent pages to this frame can inject a userspace config
  // to uniquely identify the user and limit the job to a single userspace
  // and provide webhooks
  const [namespaceConfig] = useHashParamJson<DockerJobControlConfig | undefined>("control");

  // this changes when the metaframe inputs change
  const metaframeBlob = useMetaframeAndInput();
  // important: do NOT auto serialize input blobs, since the worker is
  // the only consumer, it wastes resources
  // Output blobs tho?
  useEffect(() => {
    // This is here but currently does not seem to work:
    // https://github.com/metapages/metapage/issues/117
    if (metaframeBlob?.metaframe) {
      metaframeBlob.metaframe.isInputOutputBlobSerialization = false;
    }
  }, [metaframeBlob?.metaframe]);

  const [allowSetJob] = useOptionAllowSetJob();
  // allow setting the job definition params in the url hash via the "metaframe/job" input
  useEffect(() => {
    if (!allowSetJob || !metaframeBlob?.inputs?.["job"]) {
      return;
    }
    const jobInput = metaframeBlob.inputs["job"];
    setDefinitionParamsInUrl(jobInput);
  }, [allowSetJob, metaframeBlob.inputs, setDefinitionParamsInUrl]);

  // When all the things are updated, set the new job definition
  const setNewJobDefinition = useStore(state => state.setNewJobDefinition);

  // if the URL inputs change, or the metaframe inputs change, maybe update the store.newJobDefinition
  useEffect(() => {
    let cancelled = false;
    // So convert all possible input data types into datarefs for smallest internal representation (no big blobs)
    const definition: DockerJobDefinitionInputRefs = {
      ...definitionParamsInUrl,
    };

    const hashParams = getNonSystemHashParams();

    definition.env = {
      ...definition.env,
      ...hashParams,
    };

    // inject any hash params into the definition
    if (definition.command) {
      Object.keys(hashParams).forEach(key => {
        definition.command = definition.command.replace("${" + key + "}", hashParams[key]);
      });
    }
    if (definition.build?.context) {
      Object.keys(hashParams).forEach(key => {
        definition.build.context = definition.build.context.replace("${" + key + "}", hashParams[key]);
      });
    }

    // These are inputs set in the metaframe and stored in the url hash params. They
    // are always type: DataRefType.utf8 because they come from the text editor
    definition.configFiles = !jobInputsFromUrl
      ? {}
      : Object.fromEntries(
          Object.keys(jobInputsFromUrl).map(key => {
            return [
              key,
              {
                type: DataRefType.utf8,
                value: jobInputsFromUrl[key] as string,
              },
            ];
          }),
        );

    if (!definition.image && !definition.build) {
      return;
    }

    // sanity check
    definition.inputs = definition.inputs || {};

    (async () => {
      if (cancelled) {
        return;
      }
      // convert inputs into internal data refs so workers can consume
      // Get ALL inputs, not just the most recent, since inputs come
      // in from different sources at different times, and we accumulate them
      let inputs = { ...(metaframeBlob?.metaframe?.getInputs() || {}) };
      // This is a special input, it's the job definition, not a file input
      delete inputs["job"];

      // TODO: this shouldn't be needed, but there is a bug:
      // https://github.com/metapages/metapage/issues/117
      // This converts blobs and files into base64 strings
      inputs = await Metaframe.serializeInputs(inputs);
      if (cancelled) {
        return;
      }
      Object.keys(inputs).forEach(name => {
        const fixedName = name.startsWith("/") ? name.slice(1) : name;
        const value = inputs[name];
        // null (and undefined) cannot be serialized, so skip them
        if (value === undefined || value === null) {
          return;
        }
        if (typeof value === "object" && value?._s === true) {
          const blob = value as DataRefSerialized;
          // serialized blob/typedarray/arraybuffer
          definition.inputs![fixedName] = {
            value: blob.value,
            type: DataRefType.base64,
          };
        } else {
          // If it's a DataRef, just use it, then there's
          // no need to serialize it, or further process
          if (isDataRef(value)) {
            definition.inputs![fixedName] = value;
          } else if (typeof value === "object") {
            // assume object is a json object
            definition.inputs![fixedName] = {
              value,
              type: DataRefType.json,
            };
          } else if (typeof value === "string") {
            definition.inputs![fixedName] = {
              value,
              type: DataRefType.utf8,
            };
          } else if (typeof value === "number") {
            definition.inputs![fixedName] = {
              value: `${value}`,
              type: DataRefType.utf8,
            };
          } else {
            console.error(`I don't know how to handle input ${name}:`, value);
          }
          // TODO: is this true: Now all (non-blob) values are DataMode.utf8
        }
      });

      // at this point, these inputs *could* be very large blobs.
      // any big things are uploaded to cloud storage, then the input is replaced with a reference to the cloud lump
      const ioBaseUrl = getIOBaseUrl(resolvedQueue);

      definition.inputs = await copyLargeBlobsToCloud(definition.inputs, ioBaseUrl);
      if (cancelled) {
        return;
      }
      definition.configFiles = await copyLargeBlobsToCloud(definition.configFiles, ioBaseUrl);
      if (cancelled) {
        return;
      }

      // if uploading a large blob means new inputs have arrived and replaced this set, break out
      const jobHashCurrent = await shaDockerJob(definition);
      if (cancelled) {
        return;
      }

      const newJobDefinition: DockerJobDefinitionMetadata = {
        hash: jobHashCurrent,
        definition,
        debug,
        maxJobDuration,
        control: { ...namespaceConfig, namespace: namespaceConfig?.namespace || DefaultNamespace },
      };
      if (debug) {
        console.log("container.mtfm.io DEBUG: newJobDefinition", newJobDefinition);
      }

      setNewJobDefinition(newJobDefinition);
    })();

    return () => {
      cancelled = true;
    };
  }, [
    resolvedQueue,
    metaframeBlob.inputs,
    definitionParamsInUrl,
    jobInputsFromUrl,
    namespaceConfig,
    debug,
    maxJobDuration,
  ]);
};
