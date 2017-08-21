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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestRedisSource {
  private static final String TEST_KEY = "key1";
  private static final String REDIS_URI_TEMPLATE = "redis://localhost:%d/0";
  private RedisServer redisServer;
  private List<String> TEST_VALUE = new ArrayList<>();
  private int redisPort;

  @Before
  public void setUp() {
    try {
      redisPort = RandomPortFinder.find();
      redisServer = new RedisServer(redisPort);
      redisServer.start();
      initTestData();
    } catch (IOException e) {
      Assert.fail(e.getMessage());
      if (null != redisServer) {
        redisServer.stop();
      }
    }
  }

  @After
  public void tearDown() {
    redisServer.stop();
  }

  private RedisOriginConfigBean getOriginConfiguration() {
    RedisOriginConfigBean config = new RedisOriginConfigBean();
    config.uri = String.format(REDIS_URI_TEMPLATE, redisPort);
    config.dataFormat = DataFormat.TEXT;
    config.dataFormatConfig = new DataParserFormatConfig();
    config.connectionTimeout = 60;
    return config;
  }

  private void initTestData() {
    TEST_VALUE.add("testValue1");
    TEST_VALUE.add("testValue2");
    TEST_VALUE.add("testValue3");
    TEST_VALUE.add("testValue4");
    TEST_VALUE.add("testValue5");

    Jedis redisClient = new Jedis(URI.create(String.format(REDIS_URI_TEMPLATE, redisPort)));

    redisClient.del(TEST_KEY);
    redisClient.rpush(TEST_KEY, TEST_VALUE.toArray(new String[TEST_VALUE.size()]));

    redisClient.close();
  }

}
