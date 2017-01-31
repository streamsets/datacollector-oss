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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestElasticSearchDTargetUpgrader {

  @Test
  public void testUpgrader() throws Exception {
    StageUpgrader upgrader = new ElasticSearchDTargetUpgrader();

    List<Config> configs = new ArrayList<>();
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "clusterName", "MyCluster"));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "uris", Collections.EMPTY_LIST));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "httpUri", "http://localhost:9300"));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "useShield", false));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "useFound", false));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "clientSniff", false));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "configs", Collections.EMPTY_MAP));
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "upsert", false));

    upgrader.upgrade("l", "s", "i", 1, 6, configs);

    Assert.assertEquals(6, configs.size());
    Assert.assertEquals("elasticSearchConfigBean.timeDriver", configs.get(0).getName());
    Assert.assertEquals("elasticSearchConfigBean.timeZoneID", configs.get(1).getName());
    Assert.assertEquals("elasticSearchConfigBean.httpUris", configs.get(2).getName());
    Assert.assertEquals("http://localhost:9300", ((List<String>)configs.get(2).getValue()).get(0));
    Assert.assertEquals("elasticSearchConfigBean.useSecurity", configs.get(3).getName());
    Assert.assertEquals("elasticSearchConfigBean.params", configs.get(4).getName());
    Assert.assertEquals("elasticSearchConfigBean.defaultOperation", configs.get(5).getName());
  }

}
