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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ForceDSourceUpgraderTest {
  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/ForceDSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", null, yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV4toV5() throws StageException {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    configs.add(new Config("forceConfig.username", "v1"));
    configs.add(new Config("forceConfig.password", "v2"));
    configs.add(new Config("forceConfig.authEndpoint", "v3"));
    configs.add(new Config("forceConfig.apiVersion", "v4"));
    configs.add(new Config("forceConfig.useProxy", "v5"));
    configs.add(new Config("forceConfig.proxyHostname", "v6"));
    configs.add(new Config("forceConfig.proxyPort", "v7"));
    configs.add(new Config("forceConfig.useProxyCredentials", "v8"));
    configs.add(new Config("forceConfig.proxyRealm", "v9"));
    configs.add(new Config("forceConfig.proxyUsername", "v10"));
    configs.add(new Config("forceConfig.proxyPassword", "v11"));
    configs.add(new Config("forceConfig.mutualAuth.useMutualAuth", "v12"));
    configs.add(new Config("forceConfig.mutualAuth.useRemoteKeyStore", "v13"));
    configs.add(new Config("forceConfig.mutualAuth.keyStoreFilePath", "v14"));
    configs.add(new Config("forceConfig.mutualAuth.privateKey", "v15"));
    configs.add(new Config("forceConfig.mutualAuth.certificateChain", "v16"));
    configs.add(new Config("forceConfig.mutualAuth.keyStoreType", "v17"));
    configs.add(new Config("forceConfig.mutualAuth.keyStorePassword", "v18"));
    configs.add(new Config("forceConfig.mutualAuth.keyStoreAlgorithm", "v19"));
    configs.add(new Config("forceConfig.mutualAuth.underlyingConfig", "v20"));

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.username", "v1");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.password", "v2");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.authEndpoint", "v3");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.apiVersion", "v4");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.useProxy", "v5");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.proxyHostname", "v6");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.proxyPort", "v7");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.useProxyCredentials", "v8");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.proxyRealm", "v9");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.proxyUsername", "v10");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.proxyPassword", "v11");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.useMutualAuth", "v12");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.useRemoteKeyStore", "v13");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.keyStoreFilePath", "v14");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.privateKey", "v15");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.certificateChain", "v16");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.keyStoreType", "v17");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.keyStorePassword", "v18");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.keyStoreAlgorithm", "v19");
    UpgraderTestUtils.assertExists(configs, "forceConfig.connection.mutualAuth.underlyingConfig", "v20");

    Assert.assertEquals(20, configs.size());
  }
}
