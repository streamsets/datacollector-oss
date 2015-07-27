/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleDefinitionsJson {

  private final com.streamsets.datacollector.config.RuleDefinitions ruleDefinitions;

  @JsonCreator
  public RuleDefinitionsJson(
    @JsonProperty("metricsRuleDefinitions") List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons,
    @JsonProperty("dataRuleDefinitions") List<DataRuleDefinitionJson> dataRuleDefinitionJsons,
    @JsonProperty("emailIds") List<String> emailIds,
    @JsonProperty("uuid") UUID uuid) {
    this.ruleDefinitions = new com.streamsets.datacollector.config.RuleDefinitions(
      BeanHelper.unwrapMetricRuleDefinitions(metricsRuleDefinitionJsons),
      BeanHelper.unwrapDataRuleDefinitions(dataRuleDefinitionJsons), emailIds, uuid);
  }

  public RuleDefinitionsJson(com.streamsets.datacollector.config.RuleDefinitions ruleDefinitions) {
    this.ruleDefinitions = ruleDefinitions;
  }

  public List<MetricsRuleDefinitionJson> getMetricsRuleDefinitions() {
    return BeanHelper.wrapMetricRuleDefinitions(ruleDefinitions.getMetricsRuleDefinitions());
  }

  public List<DataRuleDefinitionJson> getDataRuleDefinitions() {
    return BeanHelper.wrapDataRuleDefinitions(ruleDefinitions.getDataRuleDefinitions());
  }

  public List<String> getEmailIds() {
    return ruleDefinitions.getEmailIds();
  }

  public List<RuleIssueJson> getRuleIssues() {
    return BeanHelper.wrapRuleIssues(ruleDefinitions.getRuleIssues());
  }

  public void setRuleIssues(List<RuleIssueJson> ruleIssueJsons) {
    //NO-OP, for jackson
  }

  public UUID getUuid() {
    return ruleDefinitions.getUuid();
  }

  @JsonIgnore
  public com.streamsets.datacollector.config.RuleDefinitions getRuleDefinitions() {
    return ruleDefinitions;
  }
}
