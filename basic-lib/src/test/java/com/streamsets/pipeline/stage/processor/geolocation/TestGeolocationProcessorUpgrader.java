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

  @Test
  public void testV2ToV3() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("geoIP2DBFile", 1000));
    configs.add(new Config("fieldTypeConverterConfigs", null));

    GeolocationProcessorUpgrader upgrader = new GeolocationProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 2, 3, configs);

    String missingAddressAction = getConfigsAsMap(configs).get("missingAddressAction").toString();
    assertEquals(missingAddressAction, "REPLACE_WITH_NULLS");
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new ArrayList<>();

    configs.add(new Config("geoIP2DBFile", "/foo/bar"));
    configs.add(new Config("geoIP2DBType", "COUNTRY"));
    configs.add(new Config("fieldTypeConverterConfigs", null));

    GeolocationProcessorUpgrader upgrader = new GeolocationProcessorUpgrader();

    upgrader.upgrade("a", "b", "c", 3, 4, configs);

    List<Map<String, String>> dbConfigs = (List)getConfigsAsMap(configs).get("dbConfigs");
    assertEquals(1, dbConfigs.size());
    assertEquals("/foo/bar", dbConfigs.get(0).get("geoIP2DBFile"));
    assertEquals("COUNTRY", dbConfigs.get(0).get("geoIP2DBType"));
  }

  private static Map<String, Object> getConfigsAsMap(List<Config> configs) {
    HashMap<String, Object> map = new HashMap<>();
    for (Config c : configs) {
      map.put(c.getName(), c.getValue());
    }
    return map;
  }
}
