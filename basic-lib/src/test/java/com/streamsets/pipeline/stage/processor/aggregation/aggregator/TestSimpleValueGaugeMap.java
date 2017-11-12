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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TestSimpleValueGaugeMap {

  @Test
  public void testMapRolling() {
    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    CountAggregator aggregator = aggregators.createSimple("count", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    aggregators.start(1L);

    aggregator.process(1L);
    provider.roll(2L);
    aggregator.process(2L);
    provider.roll(3L);
    aggregator.process(3L);

    Map map = new SimpleValueGaugeMap(WindowType.ROLLING, aggregator);

    Assert.assertFalse(map.isEmpty());

    Set<Map.Entry> set = map.entrySet();

    Assert.assertEquals(3, set.size());

    Iterator<Map.Entry> iterator = set.iterator();
    Map.Entry entry = iterator.next();
    Assert.assertEquals("1", entry.getKey());
    Assert.assertEquals(1L, entry.getValue());
    entry = iterator.next();
    Assert.assertEquals("2", entry.getKey());
    Assert.assertEquals(2L, entry.getValue());
    entry = iterator.next();
    Assert.assertEquals("3", entry.getKey());
    Assert.assertEquals(3L, entry.getValue());

    aggregator.process(1L);
    set = map.entrySet();
    Assert.assertEquals(3, set.size());
    iterator = set.iterator();
    entry = iterator.next();
    Assert.assertEquals("1", entry.getKey());
    Assert.assertEquals(1L, entry.getValue());
    entry = iterator.next();
    Assert.assertEquals("2", entry.getKey());
    Assert.assertEquals(2L, entry.getValue());
    entry = iterator.next();
    Assert.assertEquals("3", entry.getKey());
    Assert.assertEquals(4L, entry.getValue());

    aggregators.stop();
  }

  @Test
  public void testMapSliding() {
    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    CountAggregator aggregator = aggregators.createSimple("count", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    aggregators.start(1L);

    aggregator.process(1L);
    provider.roll(2L);
    aggregator.process(2L);
    provider.roll(3L);
    aggregator.process(3L);

    Map map = new SimpleValueGaugeMap(WindowType.SLIDING, aggregator);

    Assert.assertFalse(map.isEmpty());

    Set<Map.Entry> set = map.entrySet();
    Assert.assertEquals(1, set.size());
    Iterator<Map.Entry> iterator = set.iterator();
    Map.Entry entry = iterator.next();
    Assert.assertEquals("3", entry.getKey());
    Assert.assertEquals(6L, entry.getValue());

    aggregator.process(1L);


    set = map.entrySet();
    Assert.assertEquals(1, set.size());
    iterator = set.iterator();
    entry = iterator.next();
    Assert.assertEquals("3", entry.getKey());
    Assert.assertEquals(7L, entry.getValue());

    provider.roll(4L);

    set = map.entrySet();
    Assert.assertEquals(1, set.size());
    iterator = set.iterator();
    entry = iterator.next();
    Assert.assertEquals("4", entry.getKey());
    Assert.assertEquals(6L, entry.getValue());

    provider.roll(5L);

    set = map.entrySet();
    Assert.assertEquals(1, set.size());
    iterator = set.iterator();
    entry = iterator.next();
    Assert.assertEquals("5", entry.getKey());
    Assert.assertEquals(4L, entry.getValue());

    aggregators.stop();
  }

}
