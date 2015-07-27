/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.validation.RuleIssue;

import java.util.List;
import java.util.UUID;

public class RuleDefinitions {

  private final List<MetricsRuleDefinition> metricsRuleDefinitions;
  private final List<DataRuleDefinition> dataRuleDefinitions;
  private final List<String> emailIds;
  private List<RuleIssue> ruleIssues;
  private UUID uuid = null;

  public RuleDefinitions(List<MetricsRuleDefinition> metricsRuleDefinitions,
    List<DataRuleDefinition> dataRuleDefinitions, List<String> emailIds,UUID uuid) {
    this.metricsRuleDefinitions = metricsRuleDefinitions;
    this.dataRuleDefinitions = dataRuleDefinitions;
    this.emailIds = emailIds;
    this.uuid = uuid;
  }

  public List<MetricsRuleDefinition> getMetricsRuleDefinitions() {
    return metricsRuleDefinitions;
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
