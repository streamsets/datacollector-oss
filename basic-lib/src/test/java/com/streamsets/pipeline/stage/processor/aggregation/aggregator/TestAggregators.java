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
package com.streamsets.pipeline.stage.processor.aggregation.aggregator;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.stage.processor.aggregation.WindowType;
import org.junit.Assert;
import org.junit.Test;

public class TestAggregators {

  @Test
  public void testAggregators() {
    Aggregators aggregators = new Aggregators(2, WindowType.ROLLING);
    CountAggregator count = aggregators.createSimple("count", CountAggregator.class);
    GroupByAggregator<CountAggregator, Long> groupBy =
        aggregators.createGroupBy("gbCount", CountAggregator.class);
    Assert.assertNotNull(aggregators.getDataProvider());

    aggregators.start(1L);
    Assert.assertEquals(1, aggregators.getDataProvider().getDataWindows().size());
    count.process(1L);
    Assert.assertEquals((Long) 1L, count.get());
    groupBy.process("g", 1L);
    Assert.assertEquals(ImmutableMap.of("g", 1L), groupBy.get());

    aggregators.roll(2L);
    Assert.assertEquals(2, aggregators.getDataProvider().getDataWindows().size());
    count.process(2L);
    Assert.assertEquals((Long) 2L, count.get());
    groupBy.process("gg", 2L);
    Assert.assertEquals(ImmutableMap.of("gg", 2L), groupBy.get());

    aggregators.roll(3L);
    Assert.assertEquals(2, aggregators.getDataProvider().getDataWindows().size());
    count.process(3L);
    Assert.assertEquals((Long) 3L, count.get());
    groupBy.process("g", 3L);
    Assert.assertEquals(ImmutableMap.of("g", 3L), groupBy.get());
    groupBy.process("gg", 4L);
    Assert.assertEquals(ImmutableMap.of("g", 3L, "gg", 4L), groupBy.get());

    aggregators.stop();
  }
}
