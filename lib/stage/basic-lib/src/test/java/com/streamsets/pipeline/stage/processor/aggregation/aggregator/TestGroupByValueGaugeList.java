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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestGroupByValueGaugeList {

  @Test
  public void testListRolling() {
    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    GroupByAggregator<CountAggregator, Long> aggregator = aggregators.createGroupBy("gb", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    aggregators.start(1L);

    aggregator.process("a", 10L);
    provider.roll(2L);
    aggregator.process("b", 20L);
    provider.roll(3L);
    aggregator.process("c", 30L);

    List list = new GroupByValueGaugeList(WindowType.ROLLING, aggregator);

    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(1, list.size());


    Iterator<Map.Entry> iterator = list.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("a", ImmutableMap.of("1", 10L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("b", ImmutableMap.of("2", 20L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("c", ImmutableMap.of("3", 30L)), iterator.next());
    Assert.assertFalse(iterator.hasNext());

    aggregator.process("a", 40L);

    iterator = list.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("a", ImmutableMap.of("1", 10L, "3", 40L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("b", ImmutableMap.of("2", 20L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("c", ImmutableMap.of("3", 30L)), iterator.next());
    Assert.assertFalse(iterator.hasNext());

    aggregators.stop();
  }

  @Test
  public void testListSliding() {
    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    GroupByAggregator<CountAggregator, Long> aggregator = aggregators.createGroupBy("gb", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    aggregators.start(1L);

    aggregator.process("a", 10L);
    provider.roll(2L);
    aggregator.process("b", 20L);
    provider.roll(3L);
    aggregator.process("c", 30L);

    List list = new GroupByValueGaugeList(WindowType.SLIDING, aggregator);

    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(1, list.size());


    Iterator<Map.Entry> iterator = list.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("a", ImmutableMap.of("3", 10L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("b", ImmutableMap.of("3", 20L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("c", ImmutableMap.of("3", 30L)), iterator.next());
    Assert.assertFalse(iterator.hasNext());

    aggregator.process("a", 40L);

    iterator = list.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("a", ImmutableMap.of("3", 50L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("b", ImmutableMap.of("3", 20L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("c", ImmutableMap.of("3", 30L)), iterator.next());
    Assert.assertFalse(iterator.hasNext());

    provider.roll(4L);

    iterator = list.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("a", ImmutableMap.of("4", 40L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("b", ImmutableMap.of("4", 20L)), iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(ImmutableMap.of("c", ImmutableMap.of("4", 30L)), iterator.next());
    Assert.assertFalse(iterator.hasNext());

    aggregators.stop();
  }


}
