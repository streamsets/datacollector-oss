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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.GroupByAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.GroupByValueGaugeList;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.SimpleAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.SimpleValueGaugeMap;

import java.util.List;
import java.util.Map;

/**
 * An AggregatorMetric creates a Metric Gauge for an Aggregator and registers it with the pipeline metrics.
 */
public class AggregatorMetric {
  private final List gaugeData;
  private final Gauge<Map<String, Object>> gauge;

  /**
   * Constructor.
   *
   * @param context processor context.
   * @param timeWindowLabel for information purposes (to set the unit for the gauge when visualizing).
   * @param config for information purposes (to set the title for the gauge when visualizing).
   * @param aggregator the Aggregator to track with gauge.
   */
  public AggregatorMetric(
      Processor.Context context,
      WindowType windowType,
      String timeWindowLabel,
      AggregatorConfig config,
      Aggregator aggregator
  ) {
    gauge = context.createGauge(config.aggregationName);
    gauge.getValue().put("title", config.aggregationTitle);
    gauge.getValue().put("windowType", windowType.toString());
    gauge.getValue().put("timeWindow", timeWindowLabel);
    gauge.getValue().put("yAxis", config.aggregationFunction.getLabel());
    if (aggregator instanceof SimpleAggregator) {
      gaugeData = ImmutableList.of(ImmutableMap.of(
          "value",
          new SimpleValueGaugeMap(windowType, (SimpleAggregator) aggregator)
      ));
    } else {
      gaugeData = new GroupByValueGaugeList(windowType, (GroupByAggregator)aggregator);
    }
    gauge.getValue().put("data", gaugeData);
  }

  /**
   * Returns the Aggregator gauge.
   *
   * @return the Aggregator gauge.
   */
  public Gauge<Map<String, Object>> getGauge() {
    return gauge;
  }

  /**
   * Returns the data of the Aggregator gauge.
   *
   * @return the data of the Aggregator gauge.
   */
  public List getGaugeData() {
    return gaugeData;
  }

}
