import React, { useEffect, useState } from "react";
import { Tab, TabList, TabPanel, TabPanels, Tabs, Text, VStack } from "@chakra-ui/react";

import { TabConfigureFiles } from "/@/components/sections/settings/TabConfigureFiles";
import { TabConfigureImage } from "/@/components/sections/settings/TabConfigureImage";
import { TabConfigureJob } from "/@/components/sections/settings/TabConfigureJob";
import { TabConfigureDefinition } from "./TabConfigureDefinition";

export const PanelImageAndContainer: React.FC = () => {
  // local caching of user patterns
  const [tabIndex, setTabIndex] = useState<number>(
    localStorage.getItem("settings-tab") ? parseInt(localStorage.getItem("settings-tab")!) : 0,
  );
  useEffect(() => {
    localStorage.setItem("settings-tab", tabIndex.toString());
  }, [tabIndex]);

  return (
    <VStack w="100%" alignItems="stretch" overflow={"scroll"}>
      <Tabs isFitted={true} index={tabIndex} onChange={setTabIndex}>
        <TabList mb="1em">
          <Tab onClick={() => setTabIndex(0)}>
            <Text>Configure Job</Text>
          </Tab>
          <Tab>
            <Text>Docker Image</Text>
          </Tab>
          <Tab>
            <Text>Job Files</Text>
          </Tab>
          <Tab>
            <Text>Definition</Text>
          </Tab>
        </TabList>

        <TabPanels>
          <TabPanel>
            <TabConfigureJob />
          </TabPanel>
          <TabPanel>
            <TabConfigureImage />
          </TabPanel>
          <TabPanel>
            <TabConfigureFiles />
          </TabPanel>
          <TabPanel>
            <TabConfigureDefinition />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </VStack>
  );
};
