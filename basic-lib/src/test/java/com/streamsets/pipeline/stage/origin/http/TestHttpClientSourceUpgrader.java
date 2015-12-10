/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestHttpClientSourceUpgrader {

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

    HttpClientSourceUpgrader httpClientSourceUpgrader = new HttpClientSourceUpgrader();
    httpClientSourceUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(16, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("conf.dataFormat"));
    Assert.assertEquals(DataFormat.JSON, configValues.get("conf.dataFormat"));

    Assert.assertTrue(configValues.containsKey("conf.resourceUrl"));
    Assert.assertEquals("stream.twitter.com/1.1/statuses/sample.json", configValues.get("conf.resourceUrl"));

    Assert.assertTrue(configValues.containsKey("conf.httpMethod"));
    Assert.assertEquals(HttpMethod.GET, configValues.get("conf.httpMethod"));

    Assert.assertTrue(configValues.containsKey("conf.requestData"));
    Assert.assertEquals("", configValues.get("conf.requestData"));

    Assert.assertTrue(configValues.containsKey("conf.requestTimeoutMillis"));
    Assert.assertEquals(1000L, configValues.get("conf.requestTimeoutMillis"));

    Assert.assertTrue(configValues.containsKey("conf.httpMode"));
    Assert.assertEquals(HttpClientMode.STREAMING, configValues.get("conf.httpMode"));

    Assert.assertTrue(configValues.containsKey("conf.pollingInterval"));
    Assert.assertEquals(5000L, configValues.get("conf.pollingInterval"));

    Assert.assertTrue(configValues.containsKey("conf.entityDelimiter"));
    Assert.assertEquals("\n", configValues.get("conf.entityDelimiter"));

    Assert.assertTrue(configValues.containsKey("conf.authType"));
    Assert.assertEquals(AuthenticationType.OAUTH, configValues.get("conf.authType"));

    Assert.assertTrue(configValues.containsKey("conf.dataFormatConfig.jsonContent"));
    Assert.assertEquals(JsonMode.MULTIPLE_OBJECTS, configValues.get("conf.dataFormatConfig.jsonContent"));

    Assert.assertTrue(configValues.containsKey("conf.oauth.consumerKey"));
    Assert.assertEquals("MY_KEY", configValues.get("conf.oauth.consumerKey"));

    Assert.assertTrue(configValues.containsKey("conf.oauth.consumerKey"));
    Assert.assertEquals("MY_SECRET", configValues.get("conf.oauth.consumerSecret"));

    Assert.assertTrue(configValues.containsKey("conf.oauth.token"));
    Assert.assertEquals("MY_TOKEN", configValues.get("conf.oauth.token"));

    Assert.assertTrue(configValues.containsKey("conf.oauth.tokenSecret"));
    Assert.assertEquals("MY_TOKEN_SECRET", configValues.get("conf.oauth.tokenSecret"));

    Assert.assertTrue(configValues.containsKey("conf.basic.maxBatchSize"));
    Assert.assertEquals(100, configValues.get("conf.basic.maxBatchSize"));

    Assert.assertTrue(configValues.containsKey("conf.basic.maxWaitTime"));
    Assert.assertEquals(5000L, configValues.get("conf.basic.maxWaitTime"));
  }
}
