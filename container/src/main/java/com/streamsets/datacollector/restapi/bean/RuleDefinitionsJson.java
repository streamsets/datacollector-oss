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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.RuleDefinitions;

import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleDefinitionsJson {

  private final RuleDefinitions ruleDefinitions;

  @JsonCreator
  public RuleDefinitionsJson(
      @JsonProperty("schemaVersion") int schemaVersion,
      @JsonProperty("version") int version,
      @JsonProperty("metricsRuleDefinitions") List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons,
      @JsonProperty("dataRuleDefinitions") List<DataRuleDefinitionJson> dataRuleDefinitionJsons,
      @JsonProperty("driftRuleDefinitions") List<DriftRuleDefinitionJson> driftRuleDefinitionJsons,
      @JsonProperty("emailIds") List<String> emailIds,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("configuration") List<ConfigConfigurationJson> configuration
  ) {
    this.ruleDefinitions = new RuleDefinitions(
        schemaVersion,
        version,
        BeanHelper.unwrapMetricRuleDefinitions(metricsRuleDefinitionJsons),
        BeanHelper.unwrapDataRuleDefinitions(dataRuleDefinitionJsons),
        BeanHelper.unwrapDriftRuleDefinitions(driftRuleDefinitionJsons),
        emailIds,
        uuid,
        BeanHelper.unwrapConfigConfiguration(configuration)
    );
  }

  public RuleDefinitionsJson(RuleDefinitions ruleDefinitions) {
    this.ruleDefinitions = ruleDefinitions;
  }

  public int getSchemaVersion() {
    return ruleDefinitions.getSchemaVersion();
  }

  public int getVersion() {
    return ruleDefinitions.getVersion();
  }

  public List<MetricsRuleDefinitionJson> getMetricsRuleDefinitions() {
    return BeanHelper.wrapMetricRuleDefinitions(ruleDefinitions.getMetricsRuleDefinitions());
  }

  public List<DataRuleDefinitionJson> getDataRuleDefinitions() {
    return BeanHelper.wrapDataRuleDefinitions(ruleDefinitions.getDataRuleDefinitions());
  }

  public List<DriftRuleDefinitionJson> getDriftRuleDefinitions() {
    return BeanHelper.wrapDriftRuleDefinitions(ruleDefinitions.getDriftRuleDefinitions());
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

  public List<ConfigConfigurationJson> getConfiguration() {
    return BeanHelper.wrapConfigConfiguration(ruleDefinitions.getConfiguration());
  }

  public List<IssueJson> getConfigIssues() {
    return BeanHelper.wrapIssues(ruleDefinitions.getConfigIssues());
  }

  public void setConfigIssues(List<IssueJson> configIssues) {
    //NO-OP, for jackson
  }

  @JsonIgnore
  public RuleDefinitions getRuleDefinitions() {
    return ruleDefinitions;
  }
}
