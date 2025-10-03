import { type DataRef, DataRefType, DataRefTypeDefault, type InputsRefs } from "/@/shared/types.ts";

import { fetchRobust as fetch, sha256Buffer } from "/@/shared/util.ts";

import { decodeBase64 } from "/@/shared/base64.ts";

import type { DataRefSerializedBlob, MetaframeInputMap } from "@metapages/metapage";

export const ENV_VAR_DATA_ITEM_LENGTH_MAX = 200;

export const dataRefToDownloadLink = async (ref: DataRef): Promise<string> => {
  const buffer = await dataRefToBuffer(ref);
  return URL.createObjectURL(
    new Blob([buffer], { type: "application/octet-stream" }),
  );
};

export const dataRefToBuffer = async (
  ref: DataRef,
  address?: string,
): Promise<Uint8Array<ArrayBuffer>> => {
  switch (ref.type) {
    case DataRefType.base64:
      return decodeBase64(ref.value as string);
    case DataRefType.utf8:
      return new TextEncoder().encode(ref.value as string);
    case DataRefType.json:
      return new TextEncoder().encode(JSON.stringify(ref.value));
    case DataRefType.url: {
      try {
        const arrayBufferFromUrl = await urlToUint8Array(ref.value as string);
        // if (!ref.hash) {
        //   const computedHash = await sha256Buffer(arrayBufferFromUrl);
        //   ref.hash = computedHash;
        // }
        return new Uint8Array(arrayBufferFromUrl);
      } catch (downloadError) {
        console.error(
          `Failed to download data from URL ${ref.value}:`,
          downloadError,
        );
        throw downloadError;
      }
    }
    case DataRefType.key: {
      try {
        const arrayBufferFromKey = await fetchBlobFromHash(
          ref.value,
          address || "https://container.mtfm.io",
        );
        return new Uint8Array(arrayBufferFromKey);
      } catch (keyError) {
        console.error(
          `Failed to fetch blob from hash for key ${ref.value}:`,
          keyError,
        );
        throw keyError;
      }
    }
    default:
      throw new Error(
        `Not yet implemented: DataRef.type "${ref.type}" unknown`,
      );
  }
};

// Takes map of DataRefs and checks if any are too big, if so
// uploads the data to the cloud, and replaces the data ref
// with a DataRef pointing to the cloud blob
// We assume (roughly) immutable uploads based on hash
// so we keep a tally of already uploaded blobs
const AlreadyUploaded: { [address: string]: { [hash: string]: boolean } } = {};
export const copyLargeBlobsToCloud = async (
  inputs: InputsRefs | undefined,
  address: string,
): Promise<InputsRefs | undefined> => {
  if (!inputs || Object.keys(inputs).length === 0) {
    return;
  }
  const result: InputsRefs = {};

  await Promise.all(
    Object.keys(inputs).map(async (name) => {
      const type: DataRefType = inputs[name]?.type || DataRefTypeDefault;
      let uint8ArrayIfBig: Uint8Array<ArrayBuffer> | undefined;
      switch (type) {
        case DataRefType.key:
          // this is already cloud storage. no need to re-upload
          break;
        case DataRefType.url:
          // this is already somewhere else.
          break;
        case DataRefType.json:
          if (inputs?.[name]?.value) {
            const jsonString = JSON.stringify(inputs[name].value);
            if (jsonString.length > ENV_VAR_DATA_ITEM_LENGTH_MAX) {
              uint8ArrayIfBig = utf8ToBuffer(jsonString);
            }
          }
          break;
        case DataRefType.base64:
          if (inputs?.[name]?.value.length > ENV_VAR_DATA_ITEM_LENGTH_MAX) {
            uint8ArrayIfBig = decodeBase64(inputs[name].value);
          }
          break;
        case DataRefType.utf8:
          if (inputs?.[name]?.value?.length > ENV_VAR_DATA_ITEM_LENGTH_MAX) {
            uint8ArrayIfBig = utf8ToBuffer(inputs[name].value);
          }
          break;
        default:
      }

      if (uint8ArrayIfBig) {
        // upload and replace the dataref

        const hash = await sha256Buffer(uint8ArrayIfBig);
        const urlUpload = `${address}/f/${hash}`;
        // but not if we already have, since these files are immutable
        if (!AlreadyUploaded[address]) {
          AlreadyUploaded[address] = {};
        }
        if (!AlreadyUploaded[address][hash]) {
          // Then upload directly to S3/MinIO using the presigned URL
          const responseUpload = await fetch(urlUpload, {
            // @ts-ignore: TS2353
            method: "PUT",
            // @ts-ignore: TS2353
            redirect: "follow",
            body: uint8ArrayIfBig,
            headers: { "Content-Type": "application/octet-stream" },
          });
          if (!responseUpload.ok) {
            throw new Error(`Failed to get upload URL: ${urlUpload}`);
          }

          const ref: DataRef = {
            value: `${address}/f/${hash}`,
            type: DataRefType.url,
            hash: hash,
          };
          result[name] = ref; // the server gave us this ref to use
          AlreadyUploaded[address][hash] = true;
        } else {
          result[name] = {
            value: `${address}/f/${hash}`,
            type: DataRefType.url,
            hash: hash,
          };
        }
      } else {
        result[name] = inputs[name];
      }
    }),
  );
  return result;
};

// Takes map of DataRefs and converts all to desired DataMode
// e.g. gets urls and downloads to local ArrayBuffers
export const convertJobOutputDataRefsToExpectedFormat = async (
  outputs: InputsRefs | undefined,
  address: string,
): Promise<MetaframeInputMap | undefined> => {
  if (!outputs) {
    return;
  }
  let arrayBuffer: ArrayBuffer;
  const newOutputs: MetaframeInputMap = {};

  await Promise.all(
    Object.keys(outputs).map(async (name: string) => {
      const type: DataRefType = outputs[name].type || DataRefTypeDefault;
      switch (type) {
        case DataRefType.base64: {
          // well that was easy
          const internalBlobRefFromBase64: DataRefSerializedBlob = {
            _s: true,
            _c: "Blob",
            value: outputs[name].value,
            size: 0,
            fileType: undefined, // TODO: can we figure this out?
          };
          newOutputs[name] = internalBlobRefFromBase64;
          break;
        }
        case DataRefType.key: {
          arrayBuffer = await fetchBlobFromHash(outputs[name].value, address);

          const internalBlobRefFromHash: DataRefSerializedBlob = {
            _c: Blob.name,
            _s: true,
            value: bufferToBase64(arrayBuffer),
            size: arrayBuffer.byteLength,
            fileType: undefined, // TODO: can we figure this out?
          };
          newOutputs[name] = internalBlobRefFromHash;
          break;
        }
        case DataRefType.json:
          newOutputs[name] = outputs[name].value; //Unibabel.utf8ToBase64(JSON.stringify(outputs[name].value));
          break;
        case DataRefType.url: {
          arrayBuffer = await fetchBlobFromUrl(outputs[name].value);
          const internalBlobRefFromUrl: DataRefSerializedBlob = {
            _s: true,
            _c: Blob.name,
            value: bufferToBase64(arrayBuffer),
            fileType: undefined, // TODO: can we figure this out?
            size: arrayBuffer.byteLength,
          };
          newOutputs[name] = internalBlobRefFromUrl;
          break;
        }
        case DataRefType.utf8:
          newOutputs[name] = outputs[name].value; //Unibabel.utf8ToBase64(outputs[name].value);
          break;
      }
    }),
  );

  return newOutputs;
};

const fetchBlobFromUrl = async (url: string): Promise<ArrayBuffer> => {
  const response = await fetch(url, {
    // @ts-ignore: TS2353
    method: "GET",
    // @ts-ignore: TS2353
    redirect: "follow",
    headers: { "Content-Type": "application/octet-stream" },
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch URL: ${url}`);
  }
  const arrayBuffer = await response.arrayBuffer();
  return arrayBuffer;
};

export const fetchJsonFromUrl = async <T>(url: string): Promise<T> => {
  const response = await fetch(url, {
    // @ts-ignore: TS2353
    method: "GET",
    // @ts-ignore: TS2353
    redirect: "follow",
    headers: { "Content-Type": "application/json" },
  });
  const json = await response.json();
  return json;
};

export const urlToUint8Array = async (url: string): Promise<Uint8Array> => {
  // @ts-ignore: TS2353
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
  }
  const arrayBuffer = await response.arrayBuffer();
  return new Uint8Array(arrayBuffer);
};

export const fetchBlobFromHash: (
  hash: string,
  address: string,
) => Promise<ArrayBuffer> = (hash, address) => {
  return fetchBlobFromUrl(`${address}/f/${hash}`);
};

const _encoder = new TextEncoder();
export const utf8ToBuffer = (str: string): Uint8Array<ArrayBuffer> => {
  return _encoder.encode(str);
};

const _decoder = new TextDecoder();
export const bufferToUtf8 = (buffer: Uint8Array<ArrayBuffer>): string => {
  return _decoder.decode(buffer);
};

// 👍
export function bufferToBinaryString(buffer: ArrayBuffer): string {
  const base64Str = Array.prototype.map
    .call(buffer, function (ch: number) {
      return String.fromCharCode(ch);
    })
    .join("");
  return base64Str;
}

// 👍
export const bufferToBase64 = (buffer: ArrayBuffer): string => {
  const binstr = bufferToBinaryString(buffer);
  return btoa(binstr);
};
