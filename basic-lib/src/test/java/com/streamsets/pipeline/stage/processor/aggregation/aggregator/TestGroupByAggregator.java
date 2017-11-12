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

import java.util.Collections;

public class TestGroupByAggregator {

  @Test
  public void testAggregator() {
    Aggregators aggregators = new Aggregators(2, WindowType.ROLLING);
    GroupByAggregator aggregator = aggregators.createGroupBy("g", CountAggregator.class);
    aggregators.start(1);

    Aggregators aggregatorsA = new Aggregators(2, WindowType.ROLLING);
    GroupByAggregator aggregatorA = aggregatorsA.createGroupBy("g", CountAggregator.class);
    aggregatorsA.start(1);

    Assert.assertEquals("g", aggregator.getName());
    Assert.assertNotNull(aggregator.createAggregatorData(1L));

    Assert.assertEquals(Collections.emptyMap(), aggregator.get());

    aggregator.process("a", 1L);
    aggregator.process("a", 1L);
    aggregator.process("b", 1L);
    Assert.assertEquals(ImmutableMap.of("a", 2L, "b", 1L), aggregator.get());


    Assert.assertEquals("g", aggregator.getAggregatable().getName());
    Assert.assertEquals(GroupByAggregator.GroupByAggregatable.class.getSimpleName(),
        aggregator.getAggregatable().getType()
    );

    GroupByAggregator.GroupByAggregatable aggregatable =
        (GroupByAggregator.GroupByAggregatable) aggregator.getAggregatable();
    Assert.assertEquals(2, aggregatable.getGroups().size());
    Assert.assertEquals((Long) 2L, ((CountAggregator.CounterAggregatable) aggregatable.getGroups().get("a")).getCount());
    Assert.assertEquals((Long) 1L, ((CountAggregator.CounterAggregatable) aggregatable.getGroups().get("b")).getCount());

    aggregatorA.process("b", 1L);

    aggregatorA.aggregate(aggregator.getAggregatable());
    aggregatable = (GroupByAggregator.GroupByAggregatable) aggregatorA.getAggregatable();
    Assert.assertEquals(2, aggregatable.getGroups().size());

    Assert.assertEquals((Long) 2L, ((CountAggregator.CounterAggregatable) aggregatable.getGroups().get("a")).getCount());
    Assert.assertEquals((Long) 2L, ((CountAggregator.CounterAggregatable) aggregatable.getGroups().get("b")).getCount());

    aggregatorsA.stop();

    aggregators.stop();
  }

}
