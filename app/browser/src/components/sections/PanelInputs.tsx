import React, { useCallback, useEffect, useState } from "react";

import { PanelContainer } from "/@/components/generic/PanelContainer";
import { PanelHeader } from "/@/components/generic/PanelHeader";
import { AddInputButtonAndModal } from "/@/components/sections/inputs/AddInputButtonAndModal";
import { InputRow } from "/@/components/sections/inputs/InputRow";
import { useHashParamJsonObject } from "/@/hooks/useHashParamJsonObject";
import { downloadFile, getDynamicInputs } from "/@/helpers";
import { InputsRefs, JobInputs } from "/@shared/client";
import { useStore } from "/@/store";

import { Container, HStack, Icon, Spacer, Table, Tbody, Td, Text, Tr } from "@chakra-ui/react";
import { useHashParamJson } from "@metapages/hash-query/react-hooks";
import { ArrowDown } from "@phosphor-icons/react";

export const PanelInputs: React.FC = () => {
  const clientJobDefinition = useStore(state => state.newJobDefinition);
  const [jobFiles, setJobFiles] = useHashParamJsonObject<JobInputs>("files");
  // deprecated: existing URLs keep these, written to /inputs. New files are job files.
  const [jobInputs, setJobInputs] = useHashParamJson<JobInputs | undefined>("inputs");
  const [dynamicInputs, setDynamicInputs] = useState<InputsRefs>({});
  useEffect(() => {
    setDynamicInputs(getDynamicInputs(clientJobDefinition));
  }, [clientJobDefinition]);

  const addNewFile = useCallback(
    (name: string) => {
      setJobFiles({ ...jobFiles, [name]: "" });
    },
    [jobFiles, setJobFiles],
  );

  const deleteFile = useCallback(
    (name: string) => {
      const newFiles = { ...jobFiles };
      delete newFiles[name];
      setJobFiles(newFiles);
    },
    [jobFiles, setJobFiles],
  );

  const updateFile = useCallback(
    (name: string, content: string) => {
      setJobFiles({ ...jobFiles, [name]: content });
    },
    [jobFiles, setJobFiles],
  );

  const deleteLegacyInput = useCallback(
    (name: string) => {
      const newInputs = { ...jobInputs };
      delete newInputs[name];
      setJobInputs(newInputs);
    },
    [jobInputs, setJobInputs],
  );

  const updateLegacyInput = useCallback(
    (name: string, content: string) => {
      setJobInputs({ ...jobInputs, [name]: content });
    },
    [jobInputs, setJobInputs],
  );

  const fileNames: string[] = jobFiles ? Object.keys(jobFiles).sort() : [];
  const legacyInputNames: string[] = jobInputs ? Object.keys(jobInputs).sort() : [];

  return (
    <PanelContainer gap={4} boxSizing="border-box">
      <PanelHeader title={`Files + Inputs`} />
      <HStack px={4} width="100%" justifyContent="space-between">
        <Text>{`Job files, relative to the working directory (or an absolute path):`}</Text>
        <AddInputButtonAndModal add={addNewFile} showText={false} />
      </HStack>
      <Container>
        <Table px={5} variant="simple">
          <Tbody>
            {fileNames.map(name => (
              <InputRow
                key={name}
                name={name}
                content={jobFiles?.[name] ?? ""}
                onDelete={deleteFile}
                onUpdate={updateFile}
              />
            ))}
            {legacyInputNames.map(name => (
              <InputRow
                key={`inputs/${name}`}
                name={name}
                prefix="/inputs/"
                content={jobInputs?.[name] ?? ""}
                onDelete={deleteLegacyInput}
                onUpdate={updateLegacyInput}
              />
            ))}
          </Tbody>
        </Table>
      </Container>

      <HStack px={4} py={10} width="100%" justifyContent="space-between" boxSizing="border-box">
        <Text>{`/inputs/<dynamic from upstream>   (${dynamicInputs ? Object.keys(dynamicInputs).length : 0})`}</Text>
        <Spacer />
      </HStack>
      <Container>
        <Table variant="simple">
          <Tbody>
            {Object.keys(dynamicInputs).map(name => (
              <Tr key={name} justifyContent={"space-between"}>
                <Td>
                  <HStack p={2} justifyContent={"space-between"}>
                    <Text>{name}</Text>
                    <Icon
                      onClick={() => downloadFile(name, dynamicInputs[name])}
                      boxSize={"1.4rem"}
                      as={ArrowDown}></Icon>
                  </HStack>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </Container>
    </PanelContainer>
  );
};
