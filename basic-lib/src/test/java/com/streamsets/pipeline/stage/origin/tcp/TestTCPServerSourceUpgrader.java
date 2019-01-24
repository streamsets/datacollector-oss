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
package com.streamsets.pipeline.stage.origin.tcp;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestTCPServerSourceUpgrader {

  @Test
  public void testV1ToV2() throws Exception {
    List<Config> configs = new LinkedList<>();
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 1, 2, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.lengthFieldCharset");
  }

  @Test
  public void testV2ToV3() throws Exception {
    List<Config> configs = new LinkedList<>();
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();
    upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");

    configs.clear();
    upgrader.upgrade("lib", "stage", "stageInst", 1, 3, configs);
    UpgraderTestUtils.assertAllExist(configs, "conf.lengthFieldCharset");
    UpgraderTestUtils.assertAllExist(configs, "conf.readTimeout");
  }

  @Test
  public void testV3ToV4() throws Exception {
    List<Config> configs = new LinkedList<>();
    configs.add(new Config("conf.recordProcessedAckMessage", "value1"));
    configs.add(new Config("conf.batchCompletedAckMessage", "value2"));
    TCPServerSourceUpgrader upgrader = new TCPServerSourceUpgrader();

    upgrader.upgrade("lib", "stage", "stageInst", 1, 4, configs);
    Assert.assertEquals(4, configs.size());
    checkConfigurationsV3ToV4(
        "conf.recordProcessedAckMessage",
        "value1",
        "conf.batchCompletedAckMessage",
        "value2",
        configs
    );

    configs.clear();
    upgrader.upgrade("lib", "stage", "stageInst", 1, 4, configs);
    Assert.assertEquals(4, configs.size());
    checkConfigurationsV3ToV4(
        "conf.recordProcessedAckMessage",
        "Record processed",
        "conf.batchCompletedAckMessage",
        "Batch processed",
        configs
    );

    configs.clear();
    upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(2, configs.size());
    checkConfigurationsV3ToV4(
        "conf.recordProcessedAckMessage",
        "Record processed",
        "conf.batchCompletedAckMessage",
        "Batch processed",
        configs
    );
  }

  private void checkConfigurationsV3ToV4(
      @NotNull String recordsProcessedConfigName,
      @NotNull String recordsProcessedValue,
      @NotNull String batchCompletedConfigName,
      @NotNull String batchCompletedValue,
      @NotNull List<Config> configs
  ) {
    int configsFound = 0;
    for (Config config : configs) {
      if (recordsProcessedConfigName.equals(config.getName())) {
        Assert.assertEquals(recordsProcessedValue, config.getValue());
        configsFound++;
      } else if (batchCompletedConfigName.equals(config.getName())) {
        Assert.assertEquals(batchCompletedValue, config.getValue());
        configsFound++;
      }
    }

    Assert.assertEquals(2, configsFound);
  }
}
