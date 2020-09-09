/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.metadata.gen2;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestDataLakeGen2MetadataExecutorUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    upgrader = new SelectorStageUpgrader("stage", new DataLakeGen2MetadataUpgrader(), null);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1toV2withAuthUrl() throws StageException {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    // Test assuming auth token endpoint is set.
    String authUrl = "https://login.microsoftonline.com/example/oauth2/token";

    configs.add(new Config("dataLakeConfig.accountFQDN", "v1"));
    configs.add(new Config("dataLakeConfig.storageContainer", "v2"));
    configs.add(new Config("dataLakeConfig.authMethod", "OAUTH"));
    configs.add(new Config("dataLakeConfig.clientId", "v4"));
    configs.add(new Config("dataLakeConfig.clientKey", "v5"));
    configs.add(new Config("dataLakeConfig.accountKey", "v6"));
    configs.add(new Config("dataLakeConfig.secureConnection", "v7"));
    configs.add(new Config("dataLakeConfig.authTokenEndpoint", authUrl));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.accountFQDN", "v1");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.storageContainer", "v2");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.authMethod", "CLIENT");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.clientId", "v4");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.clientKey", "v5");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.accountKey", "v6");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.secureConnection", "v7");
    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.tenantId", "example");
    UpgraderTestUtils.assertNoneExist(configs, "dataLakeConfig.authTokenEndpoint");

    Assert.assertEquals(8, configs.size());
  }

  @Test
  public void testV1toV2withNoAuthUrl() throws StageException {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    // Test assuming auth token endpoint is set to old default value.
    String defaultAuthUrl = "https://login.microsoftonline.com/example-example";
    configs.add(new Config("dataLakeConfig.authTokenEndpoint", defaultAuthUrl));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "dataLakeConfig.connection.tenantId", "");
    UpgraderTestUtils.assertNoneExist(configs, "dataLakeConfig.authTokenEndpoint");
  }

}
