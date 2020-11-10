/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.pubsub.destination;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.common.CredentialsProviderType;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestPubSubTargetUpgrader {

  private static final String PUB_SUB_TARGET_CONFIG = "conf";

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/PubSubDTarget.yaml");
    upgrader = new SelectorStageUpgrader("stage", new PubSubTargetUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testUpgradeV1ToV2() {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    String dataFormatPrefix = "conf.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    Joiner p = Joiner.on(".");

    UpgraderTestUtils.assertAllExist(configs,
        p.join(PUB_SUB_TARGET_CONFIG, "requestBytesThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "elementsCountThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "defaultDelayThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "batchingEnabled"),
        p.join(PUB_SUB_TARGET_CONFIG, "maxOutstandingElementCount"),
        p.join(PUB_SUB_TARGET_CONFIG, "maxOutstandingRequestBytes"),
        p.join(PUB_SUB_TARGET_CONFIG, "limitExceededBehavior"));
  }

  @Test
  public void testUpgradeV2ToV3() {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    String prefix = "conf.credentials.";
    String newPrefix = "conf.credentials.connection.";

    String projectId = "projectId";
    String projectIdValue = "someProjectId";
    String credentialsProvider = "credentialsProvider";
    CredentialsProviderType credentialsProviderValue = CredentialsProviderType.JSON;
    String path = "path";
    String pathValue = "path";
    String credentialsFileContent = "credentialsFileContent";
    String credentialsFileContentValue = "This is the content of the credentials file";

    configs.add(new Config(prefix + projectId, projectIdValue));
    configs.add(new Config(prefix + path, pathValue));
    configs.add(new Config(prefix + credentialsFileContent, credentialsFileContentValue));
    configs.add(new Config(prefix + credentialsProvider, credentialsProviderValue));


    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertExists(configs, newPrefix + projectId, projectIdValue);
    UpgraderTestUtils.assertExists(configs, newPrefix + path, pathValue);
    UpgraderTestUtils.assertExists(configs, newPrefix + credentialsFileContent, credentialsFileContentValue);
    UpgraderTestUtils.assertExists(configs, newPrefix + credentialsProvider, credentialsProviderValue);
  }

  @Test
  public void testUpgradeV3ToV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    String prefix = "conf.";

    String maxOutstandingElementCount = "maxOutstandingElementCount";
    String maxOutstandingRequestBytes = "maxOutstandingRequestBytes";

    //If the value was 0 we will set it to the new defaults
    configs.add(new Config(prefix + maxOutstandingElementCount, 0));
    configs.add(new Config(prefix + maxOutstandingRequestBytes, 0));

    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertExists(configs, prefix + maxOutstandingElementCount, 1000);
    UpgraderTestUtils.assertExists(configs, prefix + maxOutstandingRequestBytes, 8000);

    configs.clear();

    // If the value was already changed we will use the existing values
    configs.add(new Config(prefix + maxOutstandingElementCount, 100));
    configs.add(new Config(prefix + maxOutstandingRequestBytes, 800));

    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertExists(configs, prefix + maxOutstandingElementCount, 100);
    UpgraderTestUtils.assertExists(configs, prefix + maxOutstandingRequestBytes, 800);
  }
}