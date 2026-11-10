import React, { useCallback } from "react";

import { AddJobFileButtonAndModal } from "/@/components/sections/settings/AddJobFileButtonAndModal";
import { JobFileRow } from "/@/components/sections/settings/JobFileRow";
import { useEditableJobFiles } from "/@/hooks/useEditableJobFiles";
import { useHashParamJsonObject } from "/@/hooks/useHashParamJsonObject";
import { useJobDefinitionParam } from "/@/hooks/useJobDefinitionParam";
import { JobInputs } from "/@shared/client";

import { HStack, Table, Tbody, Text, VStack } from "@chakra-ui/react";

export const TabConfigureFiles: React.FC = () => {
  const [definition] = useJobDefinitionParam();
  const [editableFiles, setFile] = useEditableJobFiles();
  const [filesInUrl, setFilesInUrl] = useHashParamJsonObject<JobInputs>("files");

  const addFile = useCallback((name: string) => setFile(name, ""), [setFile]);

  const deleteFile = useCallback(
    (name: string) => {
      const files = { ...filesInUrl };
      delete files[name];
      setFilesInUrl(Object.keys(files).length ? files : undefined);
    },
    [filesInUrl, setFilesInUrl],
  );

  const names = Object.keys({ ...definition?.files, ...filesInUrl }).sort();

  return (
    <VStack w="100%" px={4} gap={4} alignItems="stretch">
      <HStack width="100%" justifyContent="space-between">
        <Text>{`Job files, relative to the working directory (or an absolute path):`}</Text>
        <AddJobFileButtonAndModal add={addFile} showText={false} />
      </HStack>
      <Table variant="simple">
        <Tbody>
          {names.map(name => (
            <JobFileRow
              key={name}
              name={name}
              content={editableFiles[name]}
              onDelete={filesInUrl?.[name] === undefined ? undefined : deleteFile}
              onUpdate={setFile}
            />
          ))}
        </Tbody>
      </Table>
    </VStack>
  );
};
