/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

public class MetricsRuleDefinition extends RuleDefinition {

  private final String metricId;
  private final MetricType metricType;
  private final MetricElement metricElement;

  public MetricsRuleDefinition(String id, String alertText,  String metricId, MetricType metricType,
                               MetricElement metricElement, String condition,boolean sendEmail, boolean enabled) {
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
