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

import com.streamsets.pipeline.stage.processor.aggregation.WindowType;
import org.junit.Assert;
import org.junit.Test;

public class TestLongAvgAggregator {

  @Test
  public void testAggregator() {
    Aggregators aggregators = new Aggregators(2, WindowType.ROLLING);
    LongAvgAggregator aggregator = aggregators.createSimple("a", LongAvgAggregator.class);
    aggregators.start(1);

    Aggregators aggregatorsA = new Aggregators(2, WindowType.ROLLING);
    LongAvgAggregator aggregatorA = aggregatorsA.createSimple("a", LongAvgAggregator.class);
    aggregatorsA.start(1);

    Assert.assertEquals("a", aggregator.getName());
    Assert.assertNotNull(aggregator.createAggregatorData(1L));

    Assert.assertNull(aggregator.get());

    aggregator.process(1L);
    Assert.assertEquals((Long) 1L, aggregator.get());

    aggregator.process(3L);
    Assert.assertEquals((Long) 2L, aggregator.get());

    Assert.assertEquals("a", aggregator.getAggregatable().getName());
    Assert.assertEquals(LongAvgAggregator.LongAvgAggregatable.class.getSimpleName(), aggregator.getAggregatable().getType());
    Assert.assertEquals(2L, ((LongAvgAggregator.LongAvgAggregatable) aggregator.getAggregatable()).getCount());
    Assert.assertEquals(4L, ((LongAvgAggregator.LongAvgAggregatable) aggregator.getAggregatable()).getTotal());
    Assert.assertEquals(2L, ((LongAvgAggregator.LongAvgAggregatable) aggregator.getAggregatable()).getAverage());

    aggregatorA.process(1L);

    aggregatorA.aggregate(aggregator.getAggregatable());
    Assert.assertEquals((Long) 2L, aggregatorA.get());

    aggregatorsA.stop();

    aggregators.stop();
  }

}
