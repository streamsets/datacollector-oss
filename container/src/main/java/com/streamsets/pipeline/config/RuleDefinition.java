/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.validation.RuleIssue;

import java.util.List;

public class RuleDefinition {

  private final List<MetricsAlertDefinition> metricsAlertDefinitions;
  private final List<DataRuleDefinition> dataRuleDefinitions;
  private List<RuleIssue> ruleIssues;

  @JsonCreator
  public RuleDefinition(
    @JsonProperty("metricsAlertDefinitions") List<MetricsAlertDefinition> metricsAlertDefinitions,
    @JsonProperty("dataRuleDefinitions") List<DataRuleDefinition> dataRuleDefinitions) {
    this.metricsAlertDefinitions = metricsAlertDefinitions;
    this.dataRuleDefinitions = dataRuleDefinitions;
  }

  public List<MetricsAlertDefinition> getMetricsAlertDefinitions() {
    return metricsAlertDefinitions;
  }

  public List<DataRuleDefinition> getDataRuleDefinitions() {
    return dataRuleDefinitions;
  }

  public List<RuleIssue> getRuleIssues() {
    return ruleIssues;
  }

  public void setRuleIssues(List<RuleIssue> ruleIssues) {
    this.ruleIssues = ruleIssues;
  }
}
