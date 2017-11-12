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

import com.codahale.metrics.Gauge;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.AggregatorDataProvider;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregators;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.CountAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.GroupByAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestAggregatorMetric {

  @Test
  public void testSimpleAggregator() {
    Processor.Context context = ContextInfoCreator.createProcessorContext("a", false, OnRecordError.DISCARD);

    AggregatorConfig config = new AggregatorConfig();
    config.aggregationTitle = "title";
    config.aggregationFunction = AggregationFunction.COUNT;

    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    CountAggregator aggregator = aggregators.createSimple("count", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    AggregatorMetric metric = new AggregatorMetric(context, WindowType.ROLLING, "label", config, aggregator);
    aggregators.start(1L);

    Gauge<Map<String, Object>> gauge = metric.getGauge();
    List data = metric.getGaugeData();

    Assert.assertNotNull(gauge);
    Assert.assertNotNull(data);

    Assert.assertEquals(data, gauge.getValue().get("data"));

    Assert.assertEquals(0L, ((Map) ((Map) data.get(0)).get("value")).get("1"));

    aggregator.process(1L);

    Assert.assertEquals(1L, ((Map) ((Map) data.get(0)).get("value")).get("1"));

    aggregators.roll(2L);

    Assert.assertEquals(1L, ((Map) ((Map) data.get(0)).get("value")).get("1"));
    Assert.assertEquals(0L, ((Map) ((Map) data.get(0)).get("value")).get("2"));

    aggregator.process(2L);

    Assert.assertEquals(1L, ((Map) ((Map) data.get(0)).get("value")).get("1"));
    Assert.assertEquals(2L, ((Map) ((Map) data.get(0)).get("value")).get("2"));

    aggregators.stop();
  }

  @Test
  public void testGroupByAggregator() {
    Processor.Context context = ContextInfoCreator.createProcessorContext("a", false, OnRecordError.DISCARD);

    AggregatorConfig config = new AggregatorConfig();
    config.aggregationTitle = "title";
    config.aggregationFunction = AggregationFunction.COUNT;
    config.groupBy = true;

    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);
    GroupByAggregator aggregator = aggregators.createGroupBy("gb", CountAggregator.class);
    AggregatorDataProvider provider = aggregators.getDataProvider();

    AggregatorMetric metric = new AggregatorMetric(context, WindowType.ROLLING, "label", config, aggregator);
    aggregators.start(1L);

    Gauge<Map<String, Object>> gauge = metric.getGauge();
    List data = metric.getGaugeData();

    Assert.assertNotNull(gauge);
    Assert.assertNotNull(data);

    Assert.assertEquals(data, gauge.getValue().get("data"));

    Iterator iterator = data.iterator();
    Assert.assertFalse(iterator.hasNext());

    aggregator.process("a", 1L);

    iterator = data.iterator();
    Assert.assertTrue(iterator.hasNext());


    Assert.assertEquals(1L, ((Map) ((Map) iterator.next()).get("a")).get("1"));

    aggregators.roll(2L);

    iterator = data.iterator();
    Assert.assertTrue(iterator.hasNext());
    Map element = (Map) iterator.next();

    Assert.assertEquals(1L, ((Map) element.get("a")).get("1"));
    Assert.assertNull(((Map) element.get("a")).get("2"));

    Assert.assertFalse(iterator.hasNext());

    aggregator.process("a", 2L);
    aggregator.process("b", 3L);

    iterator = data.iterator();
    Assert.assertTrue(iterator.hasNext());
    element = (Map) iterator.next();

    Assert.assertEquals(1L, ((Map) element.get("a")).get("1"));
    Assert.assertEquals(2L, ((Map) element.get("a")).get("2"));

    Assert.assertTrue(iterator.hasNext());
    element = (Map) iterator.next();

    Assert.assertEquals(3L, ((Map) element.get("b")).get("2"));

    aggregators.stop();
  }

}
