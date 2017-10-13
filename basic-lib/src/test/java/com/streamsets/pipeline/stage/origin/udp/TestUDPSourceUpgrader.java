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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TestUDPSourceUpgrader {

  @Test
  public void testV2ToV3() throws Exception {
    List<Config> configs = new LinkedList<>();
    UDPSourceUpgrader upgrader = new UDPSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);
    UpgraderTestUtils.assertExists(configs, "rawDataMode", UDPSourceConfigBean.DEFAULT_RAW_DATA_MODE);
    UpgraderTestUtils.assertExists(configs, "rawDataCharset", UDPSourceConfigBean.DEFAULT_RAW_DATA_CHARSET);
    UpgraderTestUtils.assertExists(configs, "rawDataOutputField", UDPSourceConfigBean.DEFAULT_RAW_DATA_OUTPUT_FIELD);
    UpgraderTestUtils.assertExists(
        configs,
        "rawDataMultipleValuesBehavior",
        UDPSourceConfigBean.DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR
    );
    UpgraderTestUtils.assertExists(
        configs,
        "rawDataSeparatorBytes",
        UDPSourceConfigBean.DEFAULT_RAW_DATA_SEPARATOR_BYTES
    );
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new LinkedList<>();

    final List<String> ports = Arrays.asList("8934");
    final int batchSize = 42;

    configs.add(new Config("ports", ports));
    configs.add(new Config("batchSize", batchSize));
    UDPSourceUpgrader upgrader = new UDPSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    UpgraderTestUtils.assertExists(configs, UDPDSource.CONFIG_PREFIX + "ports", ports);
    UpgraderTestUtils.assertExists(configs, UDPDSource.CONFIG_PREFIX + "batchSize", batchSize);
  }
}
