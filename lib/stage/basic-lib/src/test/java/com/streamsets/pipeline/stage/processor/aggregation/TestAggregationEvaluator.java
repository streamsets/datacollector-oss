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

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregators;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

public class TestAggregationEvaluator {

  @Test
  public void testEvaluator() throws StageException {
    AggregationDProcessor processor = new AggregationDProcessor();
    processor.config = new AggregationConfigBean();
    processor.config.timeWindow = TimeWindow.TW_1D;
    processor.config.timeZoneID = "UTC";
    processor.config.timeWindowsToRemember = 1;
    processor.config.aggregatorConfigs = new ArrayList<>();
    ProcessorRunner runner =
        new ProcessorRunner.Builder(AggregationDProcessor.class, processor)
            .addOutputLane("a").build();
    Processor.Context context = (Processor.Context) runner.getContext();

    AggregatorConfig config = new AggregatorConfig();
    config.aggregationTitle = "title";
    config.aggregationFunction = AggregationFunction.COUNT;
    config.aggregationExpression = "1";

    Aggregators aggregators = new Aggregators(3, WindowType.ROLLING);

    AggregationEvaluator
        evaluator = new AggregationEvaluator(context, WindowType.ROLLING, "label", config, aggregators);

    aggregators.start(1L);
    Assert.assertEquals(config, evaluator.getConfig());
    Assert.assertNotNull(evaluator.getAggregator());
    Assert.assertNotNull(evaluator.getMetric());

    Record record = RecordCreator.create();

    evaluator.evaluate(record);

    Assert.assertEquals(1L, evaluator.getAggregator().get());

    Assert.assertEquals(1L, ((Map)((Map)evaluator.getMetric().getGaugeData().get(0)).get("value")).get("1"));

    aggregators.stop();
  }
}
