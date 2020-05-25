/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHttpServerPushSourceUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/HttpServerDPushSource.yaml");
    upgrader = new SelectorStageUpgrader("stage", new HttpServerPushSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV2() throws Exception {
    TlsConfigBeanUpgraderTestUtil.testRawKeyStoreConfigsToTlsConfigBeanUpgrade(
        "httpConfigs.",
        new HttpServerPushSourceUpgrader(),
        10
    );
  }

  @Test
  public void testV10ToV11() throws Exception {
    HttpServerPushSourceUpgrader upgrader = new HttpServerPushSourceUpgrader();
    List<Config> configs = new ArrayList<>();
    String idValue = "idFooo";
    configs.add(new Config("httpConfigs.appId", idValue));
    upgrader.upgrade("", "", "", 10, 11, configs);
    UpgraderTestUtils.assertNoneExist(configs,"httpConfigs.appId");
    UpgraderTestUtils.assertAllExist(configs, "httpConfigs.appIds");
    Config configWithName = UpgraderUtils.getConfigWithName(configs, "httpConfigs.appIds");
    List<Map<String,Object>> listCredentials = (List<Map<String,Object>>) configWithName.getValue();
    Assert.assertEquals(listCredentials.size(), 1);
    Assert.assertEquals(listCredentials.get(0).get("appId").equals(idValue), true);
  }

  @Test
  public void testV11ToV12() {
    Mockito.doReturn(11).when(context).getFromVersion();
    Mockito.doReturn(12).when(context).getToVersion();

    String dataFormatPrefix = "dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }


  @Test
  public void testV12ToV13() {
    Mockito.doReturn(12).when(context).getFromVersion();
    Mockito.doReturn(13).when(context).getToVersion();

    String configPrefix = "httpConfigs.spnegoConfigBean.";

    Config spnegoEnabledConf = new Config(configPrefix + "spnegoEnabled", true);
    Config kerberosRealmConf = new Config(configPrefix + "kerberosRealm", "FOOOO");
    configs.add(spnegoEnabledConf);
    configs.add(kerberosRealmConf);

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoEnabled", true);
    UpgraderTestUtils.assertExists(configs, configPrefix + "kerberosRealm", "FOOOO");
    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoPrincipal", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoKeytabFilePath", "");

    configs.remove(spnegoEnabledConf);
    configs.remove(kerberosRealmConf);

    configs = upgrader.upgrade(configs, context);
    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoEnabled", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "kerberosRealm", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoPrincipal", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "spnegoKeytabFilePath", "");
  }

  @Test
  public void testV13ToV14() {
    Mockito.doReturn(13).when(context).getFromVersion();
    Mockito.doReturn(14).when(context).getToVersion();

    List<Config> configs = new ArrayList<>();
    List<Map<String, Object>> appIds = new ArrayList<>();
    appIds.add(Collections.singletonMap("appId", "idFoo"));
    configs.add(new Config("httpConfigs.appIds", appIds));

    String configPrefix = "httpConfigs.tlsConfigBean.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, "httpConfigs.appIds");
    Config appIdsConfig = UpgraderUtils.getConfigWithName(configs, "httpConfigs.appIds");
    Map<String, Object> appId = ((List<Map<String, Object>>) appIdsConfig.getValue()).get(0);
    Assert.assertTrue(appId.containsKey("credential"));
    Assert.assertFalse(appId.containsKey("appId"));

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, configPrefix + "trustedCertificates", new ArrayList<>());
  }
}
