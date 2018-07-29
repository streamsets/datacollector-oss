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

import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.Aggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.CountAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.DoubleAvgAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.DoubleMaxAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.DoubleMinAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.DoubleStdDevAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.DoubleSumAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.LongAvgAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.LongMaxAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.LongMinAggregator;
import com.streamsets.pipeline.stage.processor.aggregation.aggregator.LongSumAggregator;

public enum AggregationFunction implements Label {
  COUNT("COUNT", CountAggregator.class),
  AVG_DOUBLE("AVG (double)", DoubleAvgAggregator.class),
  AVG_INTEGER("AVG (int)", LongAvgAggregator.class),
  STD_DEV("StdDev", DoubleStdDevAggregator.class),
  MIN_DOUBLE("MIN (double)", DoubleMinAggregator.class),
  MIN_INTEGER("MIN (int)", LongMinAggregator.class),
  MAX_DOUBLE("MAX (double)", DoubleMaxAggregator.class),
  MAX_INTEGER("MAX (int)", LongMaxAggregator.class),
  SUM_DOUBLE("SUM (double)", DoubleSumAggregator.class),
  SUM_INTEGER("SUM (int)", LongSumAggregator.class),
  ;

  private final String label;
  private final Class<? extends Aggregator> klass;

  AggregationFunction(String label,Class<? extends Aggregator> klass) {
    this.label = label;
    this.klass = klass;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public Class<? extends Aggregator> getAggregatorClass() {
    return klass;
  }

}
