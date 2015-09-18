/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
