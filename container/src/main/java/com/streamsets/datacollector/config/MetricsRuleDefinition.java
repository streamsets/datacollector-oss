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

public class MetricsRuleDefinition extends RuleDefinition {

  private final String metricId;
  private final MetricType metricType;
  private final MetricElement metricElement;

  public MetricsRuleDefinition(
      String id,
      String alertText,
      String metricId,
      MetricType metricType,
      MetricElement metricElement,
      String condition,
      boolean sendEmail,
      boolean enabled,
      long timestamp
  ) {
    super("METRICS", id, condition, alertText, sendEmail, enabled, timestamp);
    this.metricId = metricId;
    this.metricType = metricType;
    this.metricElement = metricElement;

  }

  public String getMetricId() {
    return metricId;
  }

  public MetricElement getMetricElement() {
    return metricElement;
  }

  public MetricType getMetricType() {
    return metricType;
  }

}
