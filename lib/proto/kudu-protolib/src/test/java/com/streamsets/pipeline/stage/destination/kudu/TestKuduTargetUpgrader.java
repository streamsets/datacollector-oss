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

package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKuduTargetUpgrader {

  @Test
  public void testUpgradeV3toV4() throws StageException {
    List<Config> configs = new ArrayList<>();
    String upsert = KuduConfigBean.CONF_PREFIX + "upsert";
    configs.add(new Config(upsert, true));
    KuduTargetUpgrader upgrader = new KuduTargetUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 3, 4, configs);
    Assert.assertEquals(1, upgradedConfigs.size());
    Config addedConf1 = upgradedConfigs.get(0);
    Assert.assertEquals("kuduConfigBean.defaultOperation", addedConf1.getName());
  }

  @Test
  public void testUpgradeV4toV5() throws StageException {
    List<Config> configs = new ArrayList<>();
    KuduTargetUpgrader upgrader = new KuduTargetUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 4, 5, configs);
    Assert.assertEquals(2, upgradedConfigs.size());
    Config addedConf1 = upgradedConfigs.get(0);
    Assert.assertEquals("kuduConfigBean.adminOperationTimeout", addedConf1.getName());
    Assert.assertEquals(30000, addedConf1.getValue());
    Config addedConf2 = upgradedConfigs.get(1);
    Assert.assertEquals("kuduConfigBean.numWorkers", addedConf2.getName());
    Assert.assertEquals(0, addedConf2.getValue());
  }
}
