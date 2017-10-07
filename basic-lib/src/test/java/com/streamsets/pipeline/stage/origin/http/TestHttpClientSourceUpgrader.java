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
package com.streamsets.pipeline.stage.origin.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.OAuthConfigBean;
import com.streamsets.pipeline.lib.http.PasswordAuthConfigBean;
import com.streamsets.pipeline.lib.http.SslConfigBean;
import com.streamsets.pipeline.stage.util.tls.TlsConfigBeanUpgraderTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHttpClientSourceUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(TestHttpClientSourceUpgrader.class);

  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("dataFormat", DataFormat.JSON));
    configs.add(new Config("resourceUrl", "stream.twitter.com/1.1/statuses/sample.json"));
    configs.add(new Config("httpMethod", HttpMethod.GET));
    configs.add(new Config("requestData", ""));
    configs.add(new Config("requestTimeoutMillis", 1000L));
    configs.add(new Config("httpMode", HttpClientMode.STREAMING));
    configs.add(new Config("pollingInterval", 5000L));
    configs.add(new Config("isOAuthEnabled", true));
    configs.add(new Config("batchSize", 100));
    configs.add(new Config("maxBatchWaitTime", 5000L));
    configs.add(new Config("consumerKey", "MY_KEY"));
    configs.add(new Config("consumerSecret", "MY_SECRET"));
    configs.add(new Config("token", "MY_TOKEN"));
    configs.add(new Config("tokenSecret", "MY_TOKEN_SECRET"));
    configs.add(new Config("jsonMode", JsonMode.MULTIPLE_OBJECTS));
    configs.add(new Config("entityDelimiter", "\n"));

    Assert.assertEquals(16, configs.size());

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(17, configs.size());

    Map<String, Object> configValues = getConfigsAsMap(configs);

    assertTrue(configValues.containsKey("conf.dataFormat"));
    Assert.assertEquals(DataFormat.JSON, configValues.get("conf.dataFormat"));

    assertTrue(configValues.containsKey("conf.resourceUrl"));
    Assert.assertEquals("stream.twitter.com/1.1/statuses/sample.json", configValues.get("conf.resourceUrl"));

    assertTrue(configValues.containsKey("conf.httpMethod"));
    Assert.assertEquals(HttpMethod.GET, configValues.get("conf.httpMethod"));

    assertTrue(configValues.containsKey("conf.requestData"));
    Assert.assertEquals("", configValues.get("conf.requestData"));

    assertTrue(configValues.containsKey("conf.requestTimeoutMillis"));
    Assert.assertEquals(1000L, configValues.get("conf.requestTimeoutMillis"));

    assertTrue(configValues.containsKey("conf.httpMode"));
    Assert.assertEquals(HttpClientMode.STREAMING, configValues.get("conf.httpMode"));

    assertTrue(configValues.containsKey("conf.pollingInterval"));
    Assert.assertEquals(5000L, configValues.get("conf.pollingInterval"));

    assertTrue(configValues.containsKey("conf.entityDelimiter"));
    Assert.assertEquals("\n", configValues.get("conf.entityDelimiter"));

    assertTrue(configValues.containsKey("conf.authType"));
    Assert.assertEquals(AuthenticationType.OAUTH, configValues.get("conf.authType"));

    assertTrue(configValues.containsKey("conf.dataFormatConfig.jsonContent"));
    Assert.assertEquals(JsonMode.MULTIPLE_OBJECTS, configValues.get("conf.dataFormatConfig.jsonContent"));

    assertTrue(configValues.containsKey("conf.oauth.consumerKey"));
    Assert.assertEquals("MY_KEY", configValues.get("conf.oauth.consumerKey"));

    assertTrue(configValues.containsKey("conf.oauth.consumerKey"));
    Assert.assertEquals("MY_SECRET", configValues.get("conf.oauth.consumerSecret"));

    assertTrue(configValues.containsKey("conf.oauth.token"));
    Assert.assertEquals("MY_TOKEN", configValues.get("conf.oauth.token"));

    assertTrue(configValues.containsKey("conf.oauth.tokenSecret"));
    Assert.assertEquals("MY_TOKEN_SECRET", configValues.get("conf.oauth.tokenSecret"));

    assertTrue(configValues.containsKey("conf.basic.maxBatchSize"));
    Assert.assertEquals(100, configValues.get("conf.basic.maxBatchSize"));

    assertTrue(configValues.containsKey("conf.basic.maxWaitTime"));
    Assert.assertEquals(5000L, configValues.get("conf.basic.maxWaitTime"));

    assertTrue(configValues.containsKey("conf.dataFormatConfig.csvSkipStartLines"));
    Assert.assertEquals(0, configValues.get("conf.dataFormatConfig.csvSkipStartLines"));

  }

  @Test
  public void testV2ToV3() throws StageException {
    List<Config> configs = new ArrayList<>();

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 2, 3, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);

    assertTrue(configValues.containsKey("conf.useProxy"));
    Assert.assertEquals(false, configValues.get("conf.useProxy"));

    assertTrue(configValues.containsKey("conf.proxy.uri"));
    Assert.assertEquals("", configValues.get("conf.proxy.uri"));
    assertTrue(configValues.containsKey("conf.proxy.username"));
    Assert.assertEquals("", configValues.get("conf.proxy.username"));
    assertTrue(configValues.containsKey("conf.proxy.password"));
    Assert.assertEquals("", configValues.get("conf.proxy.password"));
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("conf.authType", "BASIC"));

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    Assert.assertEquals(AuthenticationType.UNIVERSAL, configValues.get("conf.authType"));
  }

  @Test
  public void testV4ToV5() throws Exception {
    List<Config> configs = new ArrayList<>();

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 4, 5, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.headers"));
  }

  @Test
  public void testV5ToV6() throws Exception {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("conf.requestData", ""));

    HttpClientSourceUpgrader upgrader = new HttpClientSourceUpgrader();
    upgrader.upgrade("a", "b", "c", 5, 6, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.requestBody"));
    Assert.assertFalse(configValues.containsKey("conf.requestData"));
    Assert.assertEquals(1, configs.size());
  }

  @Test
  public void testV6ToV7() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("conf.requestTimeoutMillis", 1000));
    configs.add(new Config("conf.authType", AuthenticationType.NONE));
    configs.add(new Config("conf.oauth", new OAuthConfigBean()));
    configs.add(new Config("conf.basicAuth", new PasswordAuthConfigBean()));
    configs.add(new Config("conf.useProxy", false));
    configs.add(new Config("conf.proxy", new HttpProxyConfigBean()));
    configs.add(new Config("conf.sslConfig", new SslConfigBean()));

    HttpClientSourceUpgrader upgrader = new HttpClientSourceUpgrader();

    upgrader.upgrade("a", "b", "c", 6, 7, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    List<String> movedConfigs = ImmutableList.of(
        "conf.client.requestTimeoutMillis",
        "conf.client.authType",
        "conf.client.oauth",
        "conf.client.basicAuth",
        "conf.client.useProxy",
        "conf.client.proxy",
        "conf.client.sslConfig"
    );

    for (String config : movedConfigs) {
      boolean isPresent = configValues.containsKey(config);
      LOG.debug("{} is present: {}", config, isPresent);
      assertTrue(isPresent);
    }
  }

  @Test
  public void testV7ToV8() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("conf.entityDelimiter", "\\r\\n"));
    configs.add(new Config("conf.client.requestTimeoutMillis", 1000));

    HttpClientSourceUpgrader upgrader = new HttpClientSourceUpgrader();

    upgrader.upgrade("a", "b", "c", 7, 8, configs);
    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.client.readTimeoutMillis"));
    assertTrue(configValues.containsKey("conf.client.connectTimeoutMillis"));
    assertTrue(configValues.containsKey("conf.pagination.mode"));
    assertTrue(configValues.containsKey("conf.pagination.startAt"));
    assertTrue(configValues.containsKey("conf.pagination.resultFieldPath"));
    assertTrue(configValues.containsKey("conf.pagination.rateLimit"));
  }

  @Test
  public void testV9ToV10() throws Exception {
    List<Config> configs = new ArrayList<>();

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 9, 10, configs);

    Map<String, Object> configValues = getConfigsAsMap(configs);
    assertTrue(configValues.containsKey("conf.responseStatusActionConfigs"));
    assertEquals(HttpResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES,
            configValues.get("conf.responseTimeoutActionConfig.maxNumRetries"));
    assertEquals(HttpResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS,
            configValues.get("conf.responseTimeoutActionConfig.backoffInterval"));
    assertEquals(HttpTimeoutResponseActionConfigBean.DEFAULT_ACTION,
            configValues.get("conf.responseTimeoutActionConfig.action"));
    List<Map<String, Object>> statusActions =
            (List<Map<String, Object>>) configValues.get("conf.responseStatusActionConfigs");
    Assert.assertEquals(1, statusActions.size());
    Map<String, Object> defaultStatusAction = statusActions.get(0);
    assertEquals(HttpResponseActionConfigBean.DEFAULT_MAX_NUM_RETRIES, defaultStatusAction.get("maxNumRetries"));
    assertEquals(HttpResponseActionConfigBean.DEFAULT_BACKOFF_INTERVAL_MS, defaultStatusAction.get("backoffInterval"));
    assertEquals(HttpStatusResponseActionConfigBean.DEFAULT_ACTION, defaultStatusAction.get("action"));
    assertEquals(HttpStatusResponseActionConfigBean.DEFAULT_STATUS_CODE, defaultStatusAction.get("statusCode"));
  }

  @Test
  public void testV12ToV13() throws Exception {
    TlsConfigBeanUpgraderTestUtil.testHttpSslConfigBeanToTlsConfigBeanUpgrade(
        "conf.client.",
        new HttpClientSourceUpgrader(),
        13
    );
  }

  private static Map<String, Object> getConfigsAsMap(List<Config> configs) {
    HashMap<String, Object> map = new HashMap<>();
    for (Config c : configs) {
      map.put(c.getName(), c.getValue());
    }
    return map;
  }
}
