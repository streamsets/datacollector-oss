/*
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestGeolocationProcessorUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(TestGeolocationProcessorUpgrader.class);

  @Test
  public void testV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("geoIP2DBFile", 1000));
    configs.add(new Config("fieldTypeConverterConfigs", null));

    GeolocationProcessorUpgrader upgrader = new GeolocationProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    String geoIP2DBType = getConfigsAsMap(configs).get("geoIP2DBType").toString();
    assertEquals(geoIP2DBType, "COUNTRY");
  }

  private static Map<String, Object> getConfigsAsMap(List<Config> configs) {
    HashMap<String, Object> map = new HashMap<>();
    for (Config c : configs) {
      map.put(c.getName(), c.getValue());
    }
    return map;
  }
}
