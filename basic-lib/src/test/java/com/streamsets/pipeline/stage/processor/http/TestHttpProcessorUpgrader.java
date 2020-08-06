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
package com.streamsets.pipeline.stage.processor.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpCompressionType;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.OAuthConfigBean;
import com.streamsets.pipeline.lib.http.PasswordAuthConfigBean;
import com.streamsets.pipeline.lib.http.SslConfigBean;
import com.streamsets.pipeline.lib.http.logging.JulLogLevelChooserValues;
import com.streamsets.pipeline.lib.http.logging.RequestLoggingConfigBean;
import com.streamsets.pipeline.lib.http.logging.VerbosityChooserValues;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.origin.http.HttpClientSourceUpgrader;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import com.streamsets.pipeline.upgrader.SelectorStageUpgrader;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHttpProcessorUpgrader {

  private StageUpgrader upgrader;
  private List<Config> configs;
  private StageUpgrader.Context context;

  @Before
  public void setUp() {
    URL yamlResource = ClassLoader.getSystemClassLoader().getResource("upgrader/HttpDProcessor.yaml");
    upgrader = new SelectorStageUpgrader("stage", new HttpClientSourceUpgrader(), yamlResource);
    configs = new ArrayList<>();
    context = Mockito.mock(StageUpgrader.Context.class);
  }

  @Test
  public void testV1ToV2() throws Exception {
    configs.add(new Config("conf.requestTimeoutMillis", 1000));
    configs.add(new Config("conf.numThreads", 10));
    configs.add(new Config("conf.authType", AuthenticationType.NONE));
    configs.add(new Config("conf.oauth", new OAuthConfigBean()));
    configs.add(new Config("conf.basicAuth", new PasswordAuthConfigBean()));
    configs.add(new Config("conf.useProxy", false));
    configs.add(new Config("conf.proxy", new HttpProxyConfigBean()));
    configs.add(new Config("conf.sslConfig", new SslConfigBean()));

    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    List<String> movedConfigs = ImmutableList.of("conf.client.requestTimeoutMillis",
        "conf.client.numThreads",
        "conf.client.authType",
        "conf.client.oauth",
        "conf.client.basicAuth",
        "conf.client.useProxy",
        "conf.client.proxy",
        "conf.client.sslConfig"
    );

    for (String config : movedConfigs) {
      boolean isPresent = configValues.containsKey(config);
      assertTrue(isPresent);
    }

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.client.requestTimeoutMillis":
          Assert.assertEquals(1000, config.getValue());
          break;
        case "conf.client.authType":
          Assert.assertEquals(AuthenticationType.NONE, config.getValue());
          break;
        case "conf.client.oauth":
          Assert.assertTrue(config.getValue() instanceof OAuthConfigBean);
          break;
        case "conf.client.basicAuth":
          Assert.assertTrue(config.getValue() instanceof PasswordAuthConfigBean);
          break;
        case "conf.client.useProxy":
          Assert.assertEquals(false, config.getValue());
          break;
        case "conf.client.numThreads":
          Assert.assertEquals(10, config.getValue());
          break;
        case "conf.client.proxy":
          Assert.assertTrue(config.getValue() instanceof HttpProxyConfigBean);
          break;
        case "conf.client.sslConfig":
          Assert.assertTrue(config.getValue() instanceof SslConfigBean);
          break;
        case "conf.client.transferEncoding":
          Assert.assertEquals(RequestEntityProcessing.CHUNKED, config.getValue());
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testV2ToV3() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 2, 3, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    List<String> newConfigs = ImmutableList.of("conf.headerOutputLocation",
        "conf.headerAttributePrefix",
        "conf.headerOutputField"
    );

    for (String config : newConfigs) {
      boolean isPresent = configValues.containsKey(config);
      assertTrue(isPresent);
    }

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.headerOutputLocation":
          Assert.assertEquals(HeaderOutputLocation.HEADER.name(), config.getValue());
          break;
        case "conf.headerAttributePrefix":
          Assert.assertEquals("http-", config.getValue());
          break;
        case "conf.headerOutputField":
          Assert.assertEquals("", config.getValue());
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testV3ToV4() throws Exception {
    configs.add(new Config("conf.client.requestTimeoutMillis", "1000"));

    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 3, 4, configs);
    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.client.readTimeoutMillis"));
    assertTrue(configValues.containsKey("conf.client.connectTimeoutMillis"));
    assertTrue(configValues.containsKey("conf.maxRequestCompletionSecs"));

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.client.readTimeoutMillis":
          Assert.assertEquals("1000", config.getValue());
          break;
        case "conf.client.connectTimeoutMillis":
          Assert.assertEquals("0", config.getValue());
          break;
        case "conf.maxRequestCompletionSecs":
          Assert.assertEquals("60", config.getValue());
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testV4ToV5() throws Exception {
    configs.add(new Config("conf.dataFormatConfig.schemaInMessage", true));
    configs.add(new Config("conf.dataFormatConfig.avroSchema", null));

    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 4, 5, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.dataFormatConfig.avroSchemaSource"));
    assertTrue(configValues.containsKey("conf.dataFormatConfig.schemaRegistryUrls"));
    assertTrue(configValues.containsKey("conf.dataFormatConfig.schemaLookupMode"));
    assertTrue(configValues.containsKey("conf.dataFormatConfig.subject"));
    assertTrue(configValues.containsKey("conf.dataFormatConfig.schemaId"));

    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.dataFormatConfig.avroSchema":
          assertNull(config.getValue());
          break;
        case "conf.dataFormatConfig.subject":
          assertEquals("", config.getValue());
          break;
        case "conf.dataFormatConfig.avroSchemaSource":
          assertEquals("SOURCE", config.getValue());
          break;
        case "conf.dataFormatConfig.schemaRegistryUrls":
          assertEquals(new ArrayList<>(), config.getValue());
          break;
        case "conf.dataFormatConfig.schemaLookupMode":
          assertEquals("AUTO", config.getValue());
          break;
        case "conf.dataFormatConfig.schemaId":
          assertEquals(0, config.getValue());
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testV5ToV6() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 5, 6, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.client.useOAuth2"));
    Assert.assertEquals(false, configValues.get("conf.client.useOAuth2"));
  }

  @Test
  public void testV6ToV7() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 6, 7, configs);
    Map<String, Object> configValues = getConfigsAsMap(configs);

    assertTrue(configValues.containsKey("conf.rateLimit"));
    assertEquals(0, configValues.get("conf.rateLimit"));
  }

  @Test
  public void testV7ToV8() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 7, 8, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);

    assertTrue(configValues.containsKey("conf.client.httpCompression"));
    assertEquals(HttpCompressionType.NONE, configValues.get("conf.client.httpCompression"));
  }

  @Test
  public void testV8ToV9() throws Exception {
    TlsConfigBeanUpgraderTestUtil.testHttpSslConfigBeanToTlsConfigBeanUpgrade("conf.client.",
        new HttpProcessorUpgrader(),
        9
    );
  }

  @Test
  public void testV9ToV10() throws Exception {
    configs.add(new Config("conf.rateLimit", 500));

    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 9, 10, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);

    assertTrue(configValues.containsKey("conf.rateLimit"));
    assertEquals(2, configValues.get("conf.rateLimit"));
  }

  @Test
  public void testV10ToV11() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();
    upgrader.upgrade("lib", "stage", "inst", 10, 11, configs);

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

  private static Map<String, Object> getConfigsAsMap(List<Config> configs) {
  HashMap<String, Object> map = new HashMap<>();
    for (Config c : configs) {
      map.put(c.getName(), c.getValue());
    }
    return map;
  }

  @Test
  public void testV11ToV12() throws Exception {
    HttpProcessorUpgrader upgrader = new HttpProcessorUpgrader();
    upgrader.upgrade("lib", "stage", "inst", 11, 12, configs);

    UpgraderTestUtils.assertExists(
        configs,
        "conf.multipleValuesBehavior",
        MultipleValuesBehavior.FIRST_ONLY
    );
  }

  @Test
  public void testV12ToV13() {
    Mockito.doReturn(12).when(context).getFromVersion();
    Mockito.doReturn(13).when(context).getToVersion();

    String dataFormatPrefix = "conf.dataFormatConfig.";
    configs.add(new Config(dataFormatPrefix + "preserveRootElement", true));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "preserveRootElement", false);
  }

  @Test
  public void testV13ToV14() {
    Mockito.doReturn(13).when(context).getFromVersion();
    Mockito.doReturn(14).when(context).getToVersion();

    String dataFormatPrefix = "conf.client.";
    configs.add(new Config(dataFormatPrefix + "connectTimeoutMillis", 0));
    configs.add(new Config(dataFormatPrefix + "readTimeoutMillis", 0));
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "connectTimeoutMillis", 250000);
    UpgraderTestUtils.assertExists(configs, dataFormatPrefix + "readTimeoutMillis", 30000);
  }

  @Test
  public void testV14ToV15() {
    Mockito.doReturn(14).when(context).getFromVersion();
    Mockito.doReturn(15).when(context).getToVersion();

    String configPrefix = "conf.client.tlsConfig.";
    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs, configPrefix + "useRemoteKeyStore", false);
    UpgraderTestUtils.assertExists(configs, configPrefix + "privateKey", "");
    UpgraderTestUtils.assertExists(configs, configPrefix + "certificateChain", new ArrayList<>());
    UpgraderTestUtils.assertExists(configs, configPrefix + "trustedCertificates", new ArrayList<>());
  }

  @Test
  public void testV15ToV16() {
    Mockito.doReturn(15).when(context).getFromVersion();
    Mockito.doReturn(16).when(context).getToVersion();

    configs = upgrader.upgrade(configs, context);

    UpgraderTestUtils.assertExists(configs,"conf.missingValuesBehavior", "PASS_RECORD_ON");
  }
}
