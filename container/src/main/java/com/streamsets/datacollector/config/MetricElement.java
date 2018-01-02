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
package com.streamsets.datacollector.config;

public enum MetricElement {
  //Related to Counters
  COUNTER_COUNT,

  //Related to Histogram
  HISTOGRAM_COUNT,
  HISTOGRAM_MAX,
  HISTOGRAM_MIN,
  HISTOGRAM_MEAN,
  HISTOGRAM_MEDIAN,
  HISTOGRAM_P50,
  HISTOGRAM_P75,
  HISTOGRAM_P95,
  HISTOGRAM_P98,
  HISTOGRAM_P99,
  HISTOGRAM_P999,
  HISTOGRAM_STD_DEV,

  //Meters
  METER_COUNT,
  METER_M1_RATE,
  METER_M5_RATE,
  METER_M15_RATE,
  METER_M30_RATE,
  METER_H1_RATE,
  METER_H6_RATE,
  METER_H12_RATE,
  METER_H24_RATE,
  METER_MEAN_RATE,

  //Timer
  TIMER_COUNT,
  TIMER_MAX,
  TIMER_MIN,
  TIMER_MEAN,
  TIMER_P50,
  TIMER_P75,
  TIMER_P95,
  TIMER_P98,
  TIMER_P99,
  TIMER_P999,
  TIMER_STD_DEV,
  TIMER_M1_RATE,
  TIMER_M5_RATE,
  TIMER_M15_RATE,
  TIMER_MEAN_RATE,

  //Gauge - Related to Runtime Stats
  CURRENT_BATCH_AGE,
  TIME_IN_CURRENT_STAGE,
  TIME_OF_LAST_RECEIVED_RECORD,
  LAST_BATCH_INPUT_RECORDS_COUNT,
  LAST_BATCH_OUTPUT_RECORDS_COUNT,
  LAST_BATCH_ERROR_RECORDS_COUNT,
  LAST_BATCH_ERROR_MESSAGES_COUNT
  ;

  public boolean isOneOf(MetricElement ...elements) {
    if(elements == null) {
      return false;
    }

    for(MetricElement e: elements) {
      if(this == e) {
        return true;
      }
    }

    return false;
  }

}
