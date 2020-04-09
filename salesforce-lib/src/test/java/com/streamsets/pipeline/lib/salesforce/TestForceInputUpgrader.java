/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.stage.origin.salesforce.ForceSourceUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public abstract class TestForceInputUpgrader {
  public abstract StageUpgrader getStageUpgrader();

  // Tests common behavior of origin and lookup processor upgrade - returns the configs that
  // differ between the two
  public List<Config> testUpgradeV2toV3Common() throws StageException {
    StageUpgrader.Context context = Mockito.mock(StageUpgrader.Context.class);
    Mockito.doReturn(2).when(context).getFromVersion();
    Mockito.doReturn(3).when(context).getToVersion();

    List<Config> configs = new ArrayList<>();

    configs.add(new Config("forceConfig.usePKChunking", false));
    configs.add(new Config("forceConfig.chunkSize", 1000));
    configs.add(new Config("forceConfig.startId", "000000000000000"));

    StageUpgrader upgrader = getStageUpgrader();

    upgrader.upgrade(configs, context);

    List<Config> configsToRemove = new ArrayList<>();
    int count = 0;
    for (Config config : configs) {
      if (config.getName().startsWith("forceConfig.bulkConfig.")) {
        configsToRemove.add(config);
        count++;
      }
    }
    Assert.assertEquals("Bulk config was not properly upgraded", 3, count);
    configs.removeAll(configsToRemove);

    return configs;
  }
}
