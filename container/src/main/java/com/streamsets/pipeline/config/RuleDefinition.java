/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.validation.RuleIssue;

import java.util.ArrayList;
import java.util.List;

public class RuleDefinition {

  private final List<AlertDefinition> alertDefinitions;
  private final List<MetricsAlertDefinition> metricsAlertDefinitions;
  private final List<SamplingDefinition> samplingDefinitions;
  private final List<MetricDefinition> metricDefinitions;

  private List<RuleIssue> issues;

  @JsonCreator
  public RuleDefinition(@JsonProperty("alertDefinitions") List<AlertDefinition> alertDefinitions,
                        @JsonProperty("metricsAlertDefinitions") List<MetricsAlertDefinition> metricsAlertDefinitions,
                        @JsonProperty("samplingDefinitions") List<SamplingDefinition> samplingDefinitions,
                        @JsonProperty("counterDefinitions") List<MetricDefinition> meterDefinitions) {
    this.alertDefinitions = alertDefinitions;
    this.metricsAlertDefinitions = metricsAlertDefinitions;
    this.samplingDefinitions = samplingDefinitions;
    this.metricDefinitions = meterDefinitions;
    this.issues = new ArrayList<>();
  }

  public List<AlertDefinition> getAlertDefinitions() {
    return alertDefinitions;
  }

  public List<MetricsAlertDefinition> getMetricsAlertDefinitions() {
    return metricsAlertDefinitions;
  }

  public List<SamplingDefinition> getSamplingDefinitions() {
    return samplingDefinitions;
  }

  public List<MetricDefinition> getMetricDefinitions() {
    return metricDefinitions;
  }

  public List<RuleIssue> getIssues() {
    return issues;
  }

  public void setIssues(List<RuleIssue> issues) {
    this.issues = issues;
  }
}
