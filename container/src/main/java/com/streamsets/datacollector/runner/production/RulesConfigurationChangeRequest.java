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
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RulesConfigurationChangeRequest {

  private final RuleDefinitions ruleDefinitions;
  private final Map<String, List<DataRuleDefinition>> laneToDataRuleMap;
  private final Set<String> metricAlertsToRemove;
  private final Map<String, String> rulesToRemove;
  private final Map<String, Integer> rulesWithSampledRecordSizeChanges;

  public RulesConfigurationChangeRequest(
      RuleDefinitions ruleDefinitions,
      Map<String, String> rulesToRemove,
      Set<String> metricAlertsToRemove,
      Map<String, List<DataRuleDefinition>> laneToDataRuleMap,
      Map<String, Integer> rulesWithSampledRecordSizeChanges
  ) {
    this.ruleDefinitions = ruleDefinitions;
    this.rulesToRemove = rulesToRemove;
    this.metricAlertsToRemove = metricAlertsToRemove;
    this.laneToDataRuleMap = laneToDataRuleMap;
    this.rulesWithSampledRecordSizeChanges = rulesWithSampledRecordSizeChanges;
  }

  public RuleDefinitions getRuleDefinitions() {
    return ruleDefinitions;
  }

  public Map<String, String> getRulesToRemove() {
    return rulesToRemove;
  }

  public Set<String> getMetricAlertsToRemove() {
    return metricAlertsToRemove;
  }

  public Map<String, List<DataRuleDefinition>> getLaneToDataRuleMap() {
    return laneToDataRuleMap;
  }

  public Map<String, Integer> getRulesWithSampledRecordSizeChanges() {
    return rulesWithSampledRecordSizeChanges;
  }
}
