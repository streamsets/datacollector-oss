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

import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.RuleIssue;
import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class RuleDefinitions {
  private int schemaVersion;
  private int version;
  private final List<MetricsRuleDefinition> metricsRuleDefinitions;
  private final List<DataRuleDefinition> dataRuleDefinitions;
  private final List<DriftRuleDefinition> driftRuleDefinitions;

  @Deprecated
  private final List<String> emailIds;

  private List<RuleIssue> ruleIssues;
  private UUID uuid = null;
  private List<Config> configuration;
  private List<Issue> configIssues;

  public RuleDefinitions(
      int schemaVersion,
      int version,
      List<MetricsRuleDefinition> metricsRuleDefinitions,
      List<DataRuleDefinition> dataRuleDefinitions,
      List<DriftRuleDefinition> driftRuleDefinitions,
      List<String> emailIds,
      UUID uuid,
      List<Config> configuration
  ) {
    this.schemaVersion = schemaVersion;
    this.version = version;
    this.metricsRuleDefinitions = emptyListIfNull(metricsRuleDefinitions);
    this.dataRuleDefinitions = emptyListIfNull(dataRuleDefinitions);
    this.driftRuleDefinitions = emptyListIfNull(driftRuleDefinitions);
    this.emailIds = emailIds;
    this.uuid = uuid;
    this.configuration = configuration;
  }

  private static <T> List<T> emptyListIfNull(List<T> list) {
    return (list != null) ? list :Collections.<T>emptyList();
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(int schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public List<MetricsRuleDefinition> getMetricsRuleDefinitions() {
    return metricsRuleDefinitions;
  }

  public List<DataRuleDefinition> getDataRuleDefinitions() {
    return dataRuleDefinitions;
  }

  public List<DriftRuleDefinition> getDriftRuleDefinitions() {
    return driftRuleDefinitions;
  }

  public List<DataRuleDefinition> getAllDataRuleDefinitions() {
    List<DataRuleDefinition> rules = new ArrayList<>(getDataRuleDefinitions().size() + getDriftRuleDefinitions().size());
    rules.addAll(getDataRuleDefinitions());
    rules.addAll(getDriftRuleDefinitions());
    return rules;
  }

  @Deprecated
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

  public List<Config> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(List<Config> configuration) {
    this.configuration = configuration;
  }

  public List<Issue> getConfigIssues() {
    return configIssues;
  }

  public void setConfigIssues(List<Issue> configIssues) {
    this.configIssues = configIssues;
  }
}
