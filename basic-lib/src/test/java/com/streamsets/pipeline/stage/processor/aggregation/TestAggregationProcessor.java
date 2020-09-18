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

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    AggregationProcessor.class,
})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestAggregationProcessor {

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws StageException {

    AggregationConfigBean aggregationConfigBean = getAggregationConfigBean();

    AggregationProcessor aggregationProcessor = new AggregationProcessor(aggregationConfigBean);
    ProcessorRunner runner = new ProcessorRunner.Builder(AggregationDProcessor.class, aggregationProcessor)
        .addOutputLane("a").build();

    try {
      runner.runInit();
      Record record = RecordCreator.create();
      record.set(Field.create(true));
      StageRunner.Output output = runner.runProcess(Arrays.asList(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(true, output.getRecords().get("a").get(0).get().getValueAsBoolean());
    } finally {
      runner.runDestroy();
    }
  }

  @NotNull
  private AggregationConfigBean getAggregationConfigBean() {
    AggregationConfigBean aggregationConfigBean = new AggregationConfigBean();
    aggregationConfigBean.windowType = WindowType.ROLLING;
    aggregationConfigBean.timeWindow = TimeWindow.TW_5S;
    aggregationConfigBean.timeWindowsToRemember = 1;
    aggregationConfigBean.timeZoneID = "x";
    AggregatorConfig aggregatorConfig1 = getAggregatorConfig("a");
    aggregationConfigBean.aggregatorConfigs = Arrays.asList(aggregatorConfig1);
    return aggregationConfigBean;
  }

  @Test
  public void testInvalidAggregationConfigs() throws StageException {

    AggregationConfigBean aggregationConfigBean = new AggregationConfigBean();
    aggregationConfigBean.windowType = WindowType.ROLLING;
    aggregationConfigBean.timeWindow = TimeWindow.TW_1M;
    aggregationConfigBean.timeWindowsToRemember = 1;
    aggregationConfigBean.timeZoneID = "x";

    AggregatorConfig aggregatorConfig1 = getAggregatorConfig("a");
    AggregatorConfig aggregatorConfig2 = getAggregatorConfig("a");
    AggregatorConfig aggregatorConfig3 = getAggregatorConfig("b");
    AggregatorConfig aggregatorConfig4 = getAggregatorConfig("b");
    AggregatorConfig aggregatorConfig5 = getAggregatorConfig("a");

    AggregationProcessor aggregationProcessor = new AggregationProcessor(aggregationConfigBean);

    aggregationConfigBean.aggregatorConfigs = Arrays.asList(
        aggregatorConfig1,
        aggregatorConfig2,
        aggregatorConfig3,
        aggregatorConfig4,
        aggregatorConfig5
    );

    ProcessorRunner runner = new ProcessorRunner.Builder(AggregationDProcessor.class, aggregationProcessor)
        .addOutputLane("a").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("AGGREGATOR_00"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEventPublishingEmptyBatch() throws Exception {

    AggregationConfigBean aggregationConfigBean = getAggregationConfigBean();

    AggregationProcessor aggregationProcessor = PowerMockito.spy(new AggregationProcessor(aggregationConfigBean));
    ProcessorRunner runner = new ProcessorRunner.Builder(AggregationDProcessor.class, aggregationProcessor)
        .addOutputLane("a").build();

    try {
      runner.runInit();
      Record record = RecordCreator.create();
      record.set(Field.create(true));
      runner.runProcess(Arrays.asList());
      PowerMockito.verifyPrivate(
          aggregationProcessor,
          Mockito.times(1)
      ).invoke("publishEventRecordsIfAny");

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testGetEventRecordsQueue() {
    AggregationConfigBean aggregationConfigBean = new AggregationConfigBean();
    aggregationConfigBean.windowType = WindowType.ROLLING;
    aggregationConfigBean.timeWindow = TimeWindow.TW_5S;
    aggregationConfigBean.timeWindowsToRemember = 1;
    aggregationConfigBean.timeZoneID = "x";
    AggregatorConfig aggregatorConfig1 = getAggregatorConfig("a");
    AggregatorConfig aggregatorConfig2 = getAggregatorConfig("b");
    aggregationConfigBean.aggregatorConfigs = Arrays.asList(aggregatorConfig1, aggregatorConfig2);

    aggregationConfigBean.perAggregatorEvents = true;
    aggregationConfigBean.allAggregatorsEvent = false;

    AggregationProcessor aggregationProcessor = new AggregationProcessor(aggregationConfigBean);
    BlockingQueue<EventRecord> eventRecordsQueue = aggregationProcessor.createEventRecordsQueue();
    Assert.assertEquals(
        2*3 /*3 times the total events per close interval*/,
        eventRecordsQueue.remainingCapacity()
    );

    aggregationConfigBean.perAggregatorEvents = true;
    aggregationConfigBean.allAggregatorsEvent = true;

    aggregationProcessor = new AggregationProcessor(aggregationConfigBean);
    eventRecordsQueue = aggregationProcessor.createEventRecordsQueue();
    Assert.assertEquals(
        3*3 /*3 times the total events per close interval*/,
        eventRecordsQueue.remainingCapacity()
    );

    aggregationConfigBean.perAggregatorEvents = false;
    aggregationConfigBean.allAggregatorsEvent = true;

    aggregationProcessor = new AggregationProcessor(aggregationConfigBean);
    eventRecordsQueue = aggregationProcessor.createEventRecordsQueue();
    Assert.assertEquals(
        1*3 /*3 times the total events per close interval*/,
        eventRecordsQueue.remainingCapacity()
    );
  }


  @NotNull
  private AggregatorConfig getAggregatorConfig(String name) {
    AggregatorConfig aggregatorConfig1 = new AggregatorConfig();
    aggregatorConfig1.aggregationName = name;
    aggregatorConfig1.aggregationExpression = "X";
    aggregatorConfig1.aggregationFunction = AggregationFunction.SUM_INTEGER;
    aggregatorConfig1.aggregationTitle = "Y";
    return aggregatorConfig1;
  }

}
