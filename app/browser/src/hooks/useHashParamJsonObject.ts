import { useCallback, useMemo } from "react";

import { blobFromBase64String, blobToBase64String } from "@metapages/hash-query";
import { useHashParam } from "@metapages/hash-query/react-hooks";

/**
 * The JSON object in a hash param's value. Undefined for anything else: decoding
 * throws on a value that is not encoded JSON, such as a hand-written `#?i=3`.
 */
export const decodeHashJsonObject = <T extends object>(value?: string): T | undefined => {
  try {
    const blob = blobFromBase64String(value);
    return blob && typeof blob === "object" && !Array.isArray(blob) ? (blob as T) : undefined;
  } catch {
    return undefined;
  }
};

/** useHashParamJson, for a param that must not break the page if its value is wrong. See decodeHashJsonObject. */
export const useHashParamJsonObject = <T extends object>(key: string): [T | undefined, (blob?: T) => void] => {
  const [value, setValue] = useHashParam(key);
  const blob = useMemo(() => decodeHashJsonObject<T>(value), [value]);
  const setBlob = useCallback(
    (next?: T) => {
      setValue(next ? blobToBase64String(next) : undefined);
    },
    [setValue],
  );
  return [blob, setBlob];
};
