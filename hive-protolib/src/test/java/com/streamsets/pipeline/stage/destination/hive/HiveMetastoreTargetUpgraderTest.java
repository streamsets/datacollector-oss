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
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.api.Config;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class HiveMetastoreTargetUpgraderTest {
  @Test
  public void testUpgradeV1toV2() throws Exception {
    List<Config> configs = generateV1Configs();

    HiveMetastoreTargetUpgrader upgrader = new HiveMetastoreTargetUpgrader();
    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    for (Config config : configs) {
      assertFalse(config.getName().contains("conf.dataFormat"));
    }

  }

  private List<Config> generateV1Configs() {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("conf.dataFormat", "AVRO"));
    return configs;
  }
}
