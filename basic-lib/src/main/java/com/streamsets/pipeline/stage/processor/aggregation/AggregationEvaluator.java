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
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregators;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.GroupByAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.SimpleAggregator;

import java.util.Date;

/**
 * An AggregationEvaluator handles an aggregation defined by a configuration including the Metrics reporting.
 */
public class AggregationEvaluator {
  private final Processor.Context context;
  private final WindowType windowType;
  private final String timeWindowLabel;
  private final AggregatorConfig config;
  private final Aggregators aggregators;

  private ELEval filterEval;
  private ELEval valueEval;
  private ELEval groupByEval;
  private Aggregator aggregator;
  private boolean groupBy;
  private AggregatorMetric metric;

  public AggregationEvaluator(
      Processor.Context context,
      WindowType windowType,
      String timeWindowLabel,
      AggregatorConfig config,
      Aggregators aggregators
  ) {
    this.context = context;
    this.windowType = windowType;
    this.timeWindowLabel = timeWindowLabel;
    this.config = config;
    this.aggregators = aggregators;
    init();
  }

  protected void init() {
    AggregationFunction function = config.aggregationFunction;
    if (AggregationFunction.COUNT == function) {
      // override aggregator expression to "1" if function is COUNT
      config.aggregationExpression = "1";
    }
    if (config.filter) {
      filterEval = context.createELEval("filterPredicate");
    }
    valueEval = context.createELEval("aggregationExpression");
    groupBy = config.groupBy;
    if (groupBy) {
      groupByEval = context.createELEval("groupByExpression");
      aggregator = aggregators.createGroupBy(config.aggregationName, function.getAggregatorClass());
    } else {
      aggregator = aggregators.createSimple(config.aggregationName, function.getAggregatorClass());
    }
    metric = new AggregatorMetric(context, windowType, timeWindowLabel, config, aggregator);
  }

  public AggregatorConfig getConfig() {
    return config;
  }

  public Aggregator getAggregator() {
    return aggregator;
  }

  public AggregatorMetric getMetric() {
    return metric;
  }

  @SuppressWarnings("unchecked")
  public void evaluate(Record record) throws StageException {
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, record);
    TimeNowEL.setTimeNowInContext(vars, new Date());
    if (filterEval == null || filterEval.eval(vars, config.filterPredicate, Boolean.class)) {
      Number aggregationValue = (Number) valueEval.eval(vars, config.aggregationExpression, aggregator.getValueType());
      if (groupBy) {
        String group = groupByEval.eval(vars, config.groupByExpression, String.class);
        ((GroupByAggregator) aggregator).process(group, aggregationValue);
      } else {
        ((SimpleAggregator) aggregator).process(aggregationValue);
      }
    }
  }

}
