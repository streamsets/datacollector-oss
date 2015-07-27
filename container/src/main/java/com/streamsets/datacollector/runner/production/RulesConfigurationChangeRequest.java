/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  private final Set<String> rulesToRemove;
  private final Map<String, Integer> rulesWithSampledRecordSizeChanges;

  public RulesConfigurationChangeRequest(RuleDefinitions ruleDefinitions, Set<String> rulesToRemove,
                                         Set<String> metricAlertsToRemove,
                                         Map<String, List<DataRuleDefinition>> laneToDataRuleMap,
                                         Map<String, Integer> rulesWithSampledRecordSizeChanges) {
    this.ruleDefinitions = ruleDefinitions;
    this.rulesToRemove = rulesToRemove;
    this.metricAlertsToRemove = metricAlertsToRemove;
    this.laneToDataRuleMap = laneToDataRuleMap;
    this.rulesWithSampledRecordSizeChanges = rulesWithSampledRecordSizeChanges;
  }

  public RuleDefinitions getRuleDefinitions() {
    return ruleDefinitions;
  }

  public Set<String> getRulesToRemove() {
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
