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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.http.logging.JulLogLevelChooserValues;
import com.streamsets.pipeline.lib.http.logging.RequestLoggingConfigBean;
import com.streamsets.pipeline.lib.http.logging.VerbosityChooserValues;
import com.streamsets.pipeline.stage.destination.lib.ResponseType;
import com.streamsets.pipeline.stage.origin.http.HttpClientSourceUpgrader;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class TestHttpClientTargetUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/HttpClientDTarget.yaml");
    upgrader = new SelectorStageUpgrader("stage", new HttpClientTargetUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV2() throws Exception {
    TlsConfigBeanUpgraderTestUtil.testHttpSslConfigBeanToTlsConfigBeanUpgrade(
        "conf.client.",
        new HttpClientTargetUpgrader(),
        2
    );
  }

  @Test
  public void testV2ToV3() throws Exception {
    List<Config> configs = new ArrayList<>();

    HttpClientTargetUpgrader upgrader = new HttpClientTargetUpgrader();
    upgrader.upgrade("lib", "stage", "inst", 2, 3, configs);

    UpgraderTestUtils.assertAllExist(configs,
        "conf.client.requestLoggingConfig.enableRequestLogging",
        "conf.client.requestLoggingConfig.logLevel",
        "conf.client.requestLoggingConfig.verbosity",
        "conf.client.requestLoggingConfig.maxEntitySize"
    );

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.client.requestLoggingConfig.enableRequestLogging":
          Assert.assertEquals(false, config.getValue());
          break;
        case "conf.client.requestLoggingConfig.logLevel":
          Assert.assertEquals(JulLogLevelChooserValues.DEFAULT_LEVEL, config.getValue());
          break;
        case "conf.client.requestLoggingConfig.verbosity":
          Assert.assertEquals(VerbosityChooserValues.DEFAULT_VERBOSITY, config.getValue());
          break;
        case "conf.client.requestLoggingConfig.maxEntitySize":
          Assert.assertEquals(RequestLoggingConfigBean.DEFAULT_MAX_ENTITY_SIZE, config.getValue());
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new ArrayList<>();

    HttpClientTargetUpgrader upgrader = new HttpClientTargetUpgrader();
    upgrader.upgrade("lib", "stage", "inst", 3, 4, configs);

    UpgraderTestUtils.assertAllExist(configs,
        "conf.responseConf.sendResponseToOrigin",
        "conf.responseConf.responseType"
    );

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.responseConf.sendResponseToOrigin":
          Assert.assertEquals(false, config.getValue());
          break;
        case "conf.responseConf.responseType":
          Assert.assertEquals(ResponseType.SUCCESS_RECORDS, config.getValue());
          break;
        default:
          //do nothing
      }
    }
  }

  @Test
  public void testV4ToV5() {
    Mockito.doReturn(4).when(context).getFromVersion();
    Mockito.doReturn(5).when(context).getToVersion();

    String dataFormatPrefix = "conf.client.";
    configs.add(new Config(dataFormatPrefix + "connectTimeoutMillis", 0));
    configs.add(new Config(dataFormatPrefix + "readTimeoutMillis", 0));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "connectTimeoutMillis", 250000);
    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "readTimeoutMillis", 30000);
  }

  @Test
  public void testV5ToV6() {
    Mockito.doReturn(5).when(context).getFromVersion();
    Mockito.doReturn(6).when(context).getToVersion();

    String configPrefix = "conf.client.tlsConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, configPrefix + "trustedCertificates", new ArrayList<>());
  }
}
