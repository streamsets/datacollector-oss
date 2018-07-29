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
package com.streamsets.datacollector.creation;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.testing.pipeline.stage.TestUpgraderContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestPipelineFragmentConfigUpgrader {

  @Test
  public void testPipelineFragmentConfigUpgrader() throws StageException {
    PipelineFragmentConfigUpgrader configUpgrader = new PipelineFragmentConfigUpgrader();
    TestUpgraderContext context = new TestUpgraderContext("x", "y", "z", 1, 2);

    List<Config> upgradedConfigList = configUpgrader.upgrade(new ArrayList<>(), context);
    Assert.assertEquals(1, upgradedConfigList.size());
    Assert.assertEquals("testOriginStage", upgradedConfigList.get(0).getName());
    Assert.assertEquals(PipelineConfigBean.RAW_DATA_ORIGIN, upgradedConfigList.get(0).getValue());
  }

}
