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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestRandomDataGeneratorUpgrader {

  @Test
  public void testRandomDataGeneratorUpgrader() throws StageException {
    RandomDataGeneratorSourceUpgrader randomDataGeneratorSourceUpgrader = new RandomDataGeneratorSourceUpgrader();
    List<Config> configs = new ArrayList<>();
    randomDataGeneratorSourceUpgrader.upgrade (
      "streamsets-datacollector-dev-lib",
      "com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource",
      "com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource1432763273869",
      1,
      5,
      configs
    );

    Assert.assertEquals(3, configs.size());
    Assert.assertEquals("rootFieldType", configs.get(0).getName());
    Assert.assertEquals("MAP", configs.get(0).getValue());
    Assert.assertEquals("headerAttributes", configs.get(1).getName());
    Assert.assertTrue(configs.get(1).getValue() instanceof List);
    Assert.assertEquals(0, ((List)configs.get(1).getValue()).size());
  }
}
