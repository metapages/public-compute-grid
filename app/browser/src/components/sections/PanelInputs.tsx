import React, { useEffect, useState } from "react";

import { PanelContainer } from "/@/components/generic/PanelContainer";
import { PanelHeader } from "/@/components/generic/PanelHeader";
import { downloadFile, getDynamicInputs } from "/@/helpers";
import { InputsRefs } from "/@shared/client";
import { useStore } from "/@/store";

import { Container, HStack, Icon, Spacer, Table, Tbody, Td, Text, Tr } from "@chakra-ui/react";
import { ArrowDown } from "@phosphor-icons/react";

export const PanelInputs: React.FC = () => {
  const clientJobDefinition = useStore(state => state.newJobDefinition);
  const [dynamicInputs, setDynamicInputs] = useState<InputsRefs>({});
  useEffect(() => {
    setDynamicInputs(getDynamicInputs(clientJobDefinition));
  }, [clientJobDefinition]);

  return (
    <PanelContainer gap={4} boxSizing="border-box">
      <PanelHeader title={`Inputs`} />
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
