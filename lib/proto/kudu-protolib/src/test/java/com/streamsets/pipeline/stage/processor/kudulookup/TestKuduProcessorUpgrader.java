/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.kudulookup;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKuduProcessorUpgrader {

  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    KuduProcessorUpgrader upgrader = new KuduProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 1, 2, configs);
    Assert.assertEquals(1, upgradedConfigs.size());
    Config addedConf1 = upgradedConfigs.get(0);
    Assert.assertEquals("conf.missingLookupBehavior", addedConf1.getName());
    Assert.assertEquals(MissingValuesBehavior.SEND_TO_ERROR, addedConf1.getValue());
  }

  @Test
  public void testUpgradeV2toV3() throws StageException {
    List<Config> configs = new ArrayList<>();
    KuduProcessorUpgrader upgrader = new KuduProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);
    Assert.assertEquals(2, upgradedConfigs.size());
    Config addedConf1 = upgradedConfigs.get(0);
    Assert.assertEquals("conf.adminOperationTimeout", addedConf1.getName());
    Assert.assertEquals(30000, addedConf1.getValue());
    Config addedConf2 = upgradedConfigs.get(1);
    Assert.assertEquals("conf.numWorkers", addedConf2.getName());
    Assert.assertEquals(0, addedConf2.getValue());
  }
}
