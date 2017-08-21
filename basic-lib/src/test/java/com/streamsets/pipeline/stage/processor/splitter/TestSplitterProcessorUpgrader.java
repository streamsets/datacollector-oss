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
package com.streamsets.pipeline.stage.processor.splitter;

import com.streamsets.pipeline.api.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSplitterProcessorUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(TestSplitterProcessorUpgrader.class);

  @Test
  public void testV1ToV2() throws Exception {
    List<Config> configs = new ArrayList<>();
    SplitterProcessorUpgrader upgrader = new SplitterProcessorUpgrader();

    configs.add(new Config("separator", " "));
    upgrader.upgrade("a", "b", "c", 1, 2, configs);
    String separator = getConfigsAsMap(configs).get("separator").toString();
    assertEquals("[ ]", separator);

    configs.clear();
    configs.add(new Config("separator", "\\t"));
    upgrader.upgrade("a", "b", "c", 1, 2, configs);
    separator = getConfigsAsMap(configs).get("separator").toString();
    assertEquals("\\t", separator);

    configs.clear();
    configs.add(new Config("separator", "("));
    upgrader.upgrade("a", "b", "c", 1, 2, configs);
    separator = getConfigsAsMap(configs).get("separator").toString();
    assertEquals("\\(", separator);

  }

  private static Map<String, Object> getConfigsAsMap(List<Config> configs) {
    HashMap<String, Object> map = new HashMap<>();
    for (Config c : configs) {
      map.put(c.getName(), c.getValue());
    }
    return map;
  }
}
