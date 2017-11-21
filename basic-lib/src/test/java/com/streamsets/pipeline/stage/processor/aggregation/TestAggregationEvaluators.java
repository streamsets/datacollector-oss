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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestAggregationEvaluators {

  @Test
  public void testEvaluator() throws StageException {
    AggregationDProcessor processor = new AggregationDProcessor();
    processor.config = new AggregationConfigBean();
    processor.config.windowType = WindowType.ROLLING;
    processor.config.timeWindow = TimeWindow.TW_1D;
    processor.config.timeZoneID = "UTC";
    processor.config.timeWindowsToRemember = 1;
    processor.config.aggregatorConfigs = new ArrayList<>();
    processor.config.allAggregatorsEvent = false;
    processor.config.perAggregatorEvents = false;

    ProcessorRunner runner =
        new ProcessorRunner.Builder(AggregationDProcessor.class, processor).addOutputLane("a").build();
    Processor.Context context = (Processor.Context) runner.getContext();
    processor.config.init(context);

    AggregatorConfig agggregatorConfig = new AggregatorConfig();
    agggregatorConfig.enabled = true;
    agggregatorConfig.aggregationTitle = "title";
    agggregatorConfig.aggregationFunction = AggregationFunction.SUM_INTEGER;
    agggregatorConfig.aggregationExpression = "${record:value('/')}";
    processor.config.aggregatorConfigs = ImmutableList.of(agggregatorConfig);
    BlockingQueue<EventRecord> queue = new ArrayBlockingQueue<>(10);
    AggregationEvaluators evaluators = new AggregationEvaluators(context, processor.config, queue);
    evaluators.init();

    Assert.assertEquals(1, evaluators.getEvaluators().size());

    AggregationEvaluator evaluator = evaluators.getEvaluators().get(0);
    Assert.assertEquals(agggregatorConfig, evaluator.getConfig());
    Assert.assertNotNull(evaluator.getAggregator());
    Assert.assertNotNull(evaluator.getMetric());

    Record record = RecordCreator.create();
    record.set(Field.create(2));

    evaluators.evaluate(record);

    Assert.assertEquals(2L, evaluator.getAggregator().get());

    Assert.assertEquals(2L, ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next());

    evaluators.destroy();
  }

  @Test
  public void testCloseWindowNoEvents() throws StageException {
    AggregationDProcessor processor = new AggregationDProcessor();
    processor.config = new AggregationConfigBean();
    processor.config.windowType = WindowType.ROLLING;
    processor.config.timeWindow = TimeWindow.TW_1D;
    processor.config.timeZoneID = "UTC";
    processor.config.timeWindowsToRemember = 1;
    processor.config.aggregatorConfigs = new ArrayList<>();
    processor.config.allAggregatorsEvent = false;
    processor.config.perAggregatorEvents = false;

    ProcessorRunner runner =
        new ProcessorRunner.Builder(AggregationDProcessor.class, processor).addOutputLane("a").build();
    Processor.Context context = (Processor.Context) runner.getContext();
    processor.config.init(context);

    AggregatorConfig agggregatorConfig = new AggregatorConfig();
    agggregatorConfig.enabled = true;
    agggregatorConfig.aggregationTitle = "title";
    agggregatorConfig.aggregationFunction = AggregationFunction.COUNT;
    agggregatorConfig.aggregationExpression = "1";
    processor.config.aggregatorConfigs = ImmutableList.of(agggregatorConfig);
    BlockingQueue<EventRecord> queue = new ArrayBlockingQueue<>(10);
    AggregationEvaluators evaluators = new AggregationEvaluators(context, processor.config, queue);
    evaluators.init();

    AggregationEvaluator evaluator = evaluators.getEvaluators().get(0);

    Record record = RecordCreator.create();
    evaluators.evaluate(record);

    Assert.assertEquals(1L, ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next());

    evaluators.closeWindow();

    Assert.assertEquals(0L, ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next());

    Assert.assertTrue(queue.isEmpty());

    evaluators.destroy();
  }

  @Test
  public void testCloseWindowAllAggregatorsEvent() throws StageException, InterruptedException {
    AggregationDProcessor processor = new AggregationDProcessor();
    processor.config = new AggregationConfigBean();
    processor.config.windowType = WindowType.ROLLING;
    processor.config.timeWindow = TimeWindow.TW_1D;
    processor.config.timeZoneID = "UTC";
    processor.config.timeWindowsToRemember = 1;
    processor.config.aggregatorConfigs = new ArrayList<>();
    processor.config.allAggregatorsEvent = true;
    processor.config.perAggregatorEvents = false;

    ProcessorRunner runner =
        new ProcessorRunner.Builder(AggregationDProcessor.class, processor).addOutputLane("a").build();
    Processor.Context context = (Processor.Context) runner.getContext();
    processor.config.init(context);
    processor.config.aggregatorConfigs = getAggregationConfigs();

    BlockingQueue<EventRecord> queue = new ArrayBlockingQueue<>(10);
    AggregationEvaluators evaluators = new AggregationEvaluators(context, processor.config, queue);
    evaluators.init();

    AggregationEvaluator evaluator = evaluators.getEvaluators().get(0);

    Record record = RecordCreator.create();
    evaluators.evaluate(record);

    Assert.assertEquals(
        1L,
        ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next()
    );

    evaluators.closeWindow();

    Assert.assertEquals(
        0L,
        ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next()
    );

    Assert.assertEquals(1, queue.size());

    EventRecord event = queue.take();

    Assert.assertEquals(
        WindowType.ROLLING + AggregationEvaluators.ALL_AGGREGATORS_EVENT,
        event.getHeader().getAttribute(EventRecord.TYPE)
    );
    evaluators.destroy();
  }

  @Test
  public void testCloseWindowAggregatorsEvents() throws StageException, InterruptedException {
    AggregationDProcessor processor = new AggregationDProcessor();
    processor.config = new AggregationConfigBean();
    processor.config.windowType = WindowType.ROLLING;
    processor.config.timeWindow = TimeWindow.TW_1D;
    processor.config.timeZoneID = "UTC";
    processor.config.timeWindowsToRemember = 1;
    processor.config.aggregatorConfigs = new ArrayList<>();
    processor.config.allAggregatorsEvent = false;
    processor.config.perAggregatorEvents = true;

    ProcessorRunner runner =
        new ProcessorRunner.Builder(AggregationDProcessor.class, processor).addOutputLane("a").build();
    Processor.Context context = (Processor.Context) runner.getContext();
    processor.config.init(context);

    processor.config.aggregatorConfigs = getAggregationConfigs();

    BlockingQueue<EventRecord> queue = new ArrayBlockingQueue<>(10);
    AggregationEvaluators evaluators = new AggregationEvaluators(context, processor.config, queue);
    evaluators.init();

    AggregationEvaluator evaluator = evaluators.getEvaluators().get(0);

    Record record = RecordCreator.create();
    evaluators.evaluate(record);

    Assert.assertEquals(
        1L,
        ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next()
    );

    evaluators.closeWindow();

    Assert.assertEquals(
        0L,
        ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).values().iterator().next()
    );

    // Make sure that 2 records are produced, 1 per each aggregation config
    Assert.assertEquals(2, queue.size());

    EventRecord event = queue.take();
    Assert.assertEquals(
        WindowType.ROLLING + AggregationEvaluators.SINGLE_AGGREGATOR_EVENT,
        event.getHeader().getAttribute(EventRecord.TYPE)
    );

    event = queue.take();
    Assert.assertEquals(
        WindowType.ROLLING + AggregationEvaluators.SINGLE_AGGREGATOR_EVENT,
        event.getHeader().getAttribute(EventRecord.TYPE)
    );
    evaluators.destroy();
  }

  public List<AggregatorConfig> getAggregationConfigs() {
    AggregatorConfig aggregatorConfig1 = new AggregatorConfig();
    aggregatorConfig1.aggregationName = "agg1";
    aggregatorConfig1.enabled = true;
    aggregatorConfig1.aggregationTitle = "title1";
    aggregatorConfig1.aggregationFunction = AggregationFunction.COUNT;
    aggregatorConfig1.aggregationExpression = "1";

    AggregatorConfig aggregatorConfig2 = new AggregatorConfig();
    aggregatorConfig2.aggregationName = "agg2";
    aggregatorConfig2.enabled = true;
    aggregatorConfig2.aggregationTitle = "title2";
    aggregatorConfig2.aggregationFunction = AggregationFunction.COUNT;
    aggregatorConfig2.aggregationExpression = "1";
    return ImmutableList.of(aggregatorConfig1, aggregatorConfig2);
  }
}
