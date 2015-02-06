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
import java.util.UUID;

public class RuleDefinition {

  private final List<MetricsAlertDefinition> metricsAlertDefinitions;
  private final List<DataRuleDefinition> dataRuleDefinitions;
  private final List<String> emailIds;
  private List<RuleIssue> ruleIssues;
  private UUID uuid = null;

  @JsonCreator
  public RuleDefinition(
    @JsonProperty("metricsAlertDefinitions") List<MetricsAlertDefinition> metricsAlertDefinitions,
    @JsonProperty("dataRuleDefinitions") List<DataRuleDefinition> dataRuleDefinitions,
    @JsonProperty("emailIds") List<String> emailIds,
    @JsonProperty("uuid") UUID uuid) {
    this.metricsAlertDefinitions = metricsAlertDefinitions;
    this.dataRuleDefinitions = dataRuleDefinitions;
    this.emailIds = emailIds;
    this.uuid = uuid;
  }

  public List<MetricsAlertDefinition> getMetricsAlertDefinitions() {
    return metricsAlertDefinitions;
  }

  public List<DataRuleDefinition> getDataRuleDefinitions() {
    return dataRuleDefinitions;
  }

  public List<String> getEmailIds() {
    return emailIds;
  }

  public List<RuleIssue> getRuleIssues() {
    return ruleIssues;
  }

  public void setRuleIssues(List<RuleIssue> ruleIssues) {
    this.ruleIssues = ruleIssues;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public UUID getUuid() {
    return uuid;
  }

}
