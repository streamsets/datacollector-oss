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
package com.streamsets.pipeline.stage.origin.restservice;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestRestServicePushSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    upgrader = new SelectorStageUpgrader("stage", new RestServicePushSourceUpgrader(), null);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV2() throws Exception {
    Mockito.doReturn(1).when(context).getFromVersion();
    Mockito.doReturn(2).when(context).getToVersion();

    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "httpConfigs.tlsConfigBean.trustStoreFilePath",
        "httpConfigs.tlsConfigBean.trustStoreType",
        "httpConfigs.tlsConfigBean.trustStorePassword",
        "httpConfigs.tlsConfigBean.trustStoreAlgorithm",
        "httpConfigs.needClientAuth"
    );
  }

  @Test
  public void testV2ToV3() throws Exception {
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "responseConfig.sendRawResponse"
    );
  }

  @Test
  public void testV3ToV4() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertAllExist(
        configs,
        "dataFormatConfig.preserveRootElement"
    );
  }

  @Test
  public void testV4ToV5() {
    Mockito.doReturn(3).when(context).getFromVersion();
    Mockito.doReturn(4).when(context).getToVersion();

    String idValue = "idFooo";
    configs.add(new Config("httpConfigs.appId", idValue));
    upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertNoneExist(configs,"httpConfigs.appId");
    UpgraderTestUtils.assertAllExist(configs, "httpConfigs.appIds");
    Config configWithName = UpgraderUtils.getConfigWithName(configs, "httpConfigs.appIds");
    List<Map<String,Object>> listCredentials = (List<Map<String,Object>>) configWithName.getValue();
    Assert.assertEquals(listCredentials.size(), 1);
    Assert.assertEquals(listCredentials.get(0).get("appId"), idValue);
  }
}
