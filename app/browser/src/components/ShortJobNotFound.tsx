import React from "react";

import { getShortJobIdFromPath, useShortJobStore } from "/@/hooks/useJobDefinitionParam";

import { Alert, AlertIcon, Link, Text } from "@chakra-ui/react";

export const ShortJobNotFound: React.FC = () => {
  const notFound = useShortJobStore(s => s.shortJobNotFound);
  if (!notFound) {
    return null;
  }
  const jobId = getShortJobIdFromPath();
  const search = new URLSearchParams(window.location.search);
  return (
    <Alert status="warning">
      <AlertIcon />
      {search.has("i") || search.has("v") ? (
        <Text>
          This job has not been submitted with these inputs or this version, or they have expired.{" "}
          <Link textDecoration="underline" href={`/j/${jobId}${window.location.hash}`}>
            Open the job on its own
          </Link>
        </Text>
      ) : (
        <Text>This job was not found: it may have expired.</Text>
      )}
    </Alert>
  );
};
