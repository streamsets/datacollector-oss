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
import java.util.List;

public class TestElasticSearchDTargetUpgrader {

  @Test
  public void testUpgraderV1toV2() throws Exception {
    StageUpgrader upgrader = new ElasticSearchDTargetUpgrader();

    List<Config> configs = new ArrayList<>();

    upgrader.upgrade("l", "s", "i", 1, 2, configs);

    Assert.assertEquals(2, configs.size());
    Assert.assertEquals(new Config("timeDriver", "${time:now()}").getName(), configs.get(0).getName());
    Assert.assertEquals(new Config("timeZoneID", "UTC").getName(), configs.get(1).getName());
    Assert.assertEquals(new Config("timeDriver", "${time:now()}").getValue(), configs.get(0).getValue());
    Assert.assertEquals(new Config("timeZoneID", "UTC").getValue(), configs.get(1).getValue());
  }

}
