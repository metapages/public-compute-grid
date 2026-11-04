import React, { useCallback } from "react";

import { ButtonDeleteWithConfirm } from "/@/components/generic/ButtonDeleteWithConfirm";
import { ButtonModalEditor } from "/@/components/generic/ButtonModalEditor";

import { Flex, HStack, Icon, Td, Text, Tr } from "@chakra-ui/react";
import { File } from "@phosphor-icons/react";

export const JobFileRow: React.FC<{
  name: string;
  // undefined when it is not text, so cannot be edited here
  content?: string;
  // undefined when it is part of the definition, so cannot be deleted here
  onDelete?: (name: string) => void;
  onUpdate: (name: string, content: string) => void;
}> = ({ name, content, onDelete, onUpdate }) => {
  const onUpdateMemoized = useCallback((contentUpdate: string) => onUpdate(name, contentUpdate), [name, onUpdate]);

  return (
    <Tr>
      <Td>
        <HStack gap={0}>
          <Icon as={File} mr={3}></Icon>
          <Text as="span">{name}</Text>
        </HStack>
      </Td>
      <Td>
        <Flex align={"center"} justify={"flex-end"} gap={3}>
          {content === undefined ? null : (
            <ButtonModalEditor fileName={name} content={content} onUpdate={onUpdateMemoized} />
          )}
          {onDelete ? <ButtonDeleteWithConfirm callback={() => onDelete(name)} /> : null}
        </Flex>
      </Td>
    </Tr>
  );
};
