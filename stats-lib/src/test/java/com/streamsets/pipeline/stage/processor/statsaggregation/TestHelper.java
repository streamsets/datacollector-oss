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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Record;

import java.util.Arrays;
import java.util.List;

public class TestHelper {

  private static final String AGGREGATOR = "Aggregator";
  private static final String STATS_AGGREGATOR_TARGET = "StatsAggregatorTarget";

  public static  Record createRecord(String recordSourceId) {
    RecordImpl record = new RecordImpl(AGGREGATOR, recordSourceId, null, null);
    record.addStageToStagePath(STATS_AGGREGATOR_TARGET);
    record.createTrackingId();
    return record;
  }

  public static  MetricsRuleDefinition createTestMetricRuleDefinition(String condition, boolean enabled, long timestamp) {
    return new MetricsRuleDefinition(
      "x",
      "y",
      "z",
      MetricType.COUNTER,
      MetricElement.COUNTER_COUNT,
      condition,
      false,
      enabled,
      timestamp
    );
  }

  public static  DataRuleDefinition createTestDataRuleDefinition(String condition, boolean enabled, long timestamp) {
    return new DataRuleDefinition(
      "f",
      "x",
      "label",
      "lane",
      50,
      10,
      condition,
      true,
      "x",
      ThresholdType.COUNT,
      "100",
      100,
      true,
      true,
      enabled,
      timestamp
    );
  }


  public static  List<Record> createTestDataRuleRecords() {
    return Arrays.asList(
      AggregatorUtil.createDataRuleRecord(
        "x",
        "x",
        750,
        100,
        Arrays.asList("Alert!!"),
        false
      ),
      AggregatorUtil.createDataRuleRecord(
        "x",
        "x",
        1000,
        200,
        Arrays.asList("Alert!!"),
        false
      ),
      AggregatorUtil.createDataRuleRecord(
        "x",
        "x",
        250,
        100,
        Arrays.asList("Alert!!"),
        false
      )
    );
  }
}
