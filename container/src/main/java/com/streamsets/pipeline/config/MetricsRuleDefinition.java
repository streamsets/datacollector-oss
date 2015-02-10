/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricsRuleDefinition extends RuleDefinition {

  private final String metricId;
  private final MetricType metricType;
  private final MetricElement metricElement;

  @JsonCreator
  public MetricsRuleDefinition(@JsonProperty("id") String id,
                               @JsonProperty("alertText") String alertText,
                               @JsonProperty("metricId") String metricId,
                               @JsonProperty("metricType") MetricType metricType,
                               @JsonProperty("metricElement") MetricElement metricElement,
                               @JsonProperty("condition") String condition,
                               @JsonProperty("sendMail") boolean sendEmail,
                               @JsonProperty("enabled") boolean enabled) {
    super(id, condition, alertText, sendEmail, enabled);
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
