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
package com.streamsets.pipeline.stage.processor.aggregation;

import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestAggregationProcessorUpgrader {
  @Test
  public void testUpgradeV2toV3() throws Exception {

    List<Config> configs = new ArrayList<>();
    AggregationProcessorUpgrader aggregationProcessorUpgrader = new AggregationProcessorUpgrader();
    aggregationProcessorUpgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals(1, configs.size());
    Config config = configs.get(0);
    Assert.assertEquals(
        AggregationProcessorUpgrader.JOINER.join(
            AggregationProcessorUpgrader.CONFIG,
            AggregationProcessorUpgrader.EVENT_RECORD_TEXT_FIELD
        ),
        config.getName()
    );
    Assert.assertEquals(true, config.getValue());
  }
}
