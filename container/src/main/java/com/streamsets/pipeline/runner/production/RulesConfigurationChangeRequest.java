/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.config.DataAlertDefinition;
import com.streamsets.pipeline.config.RuleDefinition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RulesConfigurationChangeRequest {

  private final RuleDefinition ruleDefinition;
  private final Map<String, List<DataAlertDefinition>> laneToDataRuleMap;
  private final Set<String> metricAlertsToRemove;
  private final Set<String> rulesToRemove;

  public RulesConfigurationChangeRequest(RuleDefinition ruleDefinition, Set<String> rulesToRemove,
                                         Set<String> metricAlertsToRemove,
                                         Map<String, List<DataAlertDefinition>> laneToDataRuleMap) {
    this.ruleDefinition = ruleDefinition;
    this.rulesToRemove = rulesToRemove;
    this.metricAlertsToRemove = metricAlertsToRemove;
    this.laneToDataRuleMap = laneToDataRuleMap;
  }

  public RuleDefinition getRuleDefinition() {
    return ruleDefinition;
  }

  public Set<String> getRulesToRemove() {
    return rulesToRemove;
  }

  public Set<String> getMetricAlertsToRemove() {
    return metricAlertsToRemove;
  }

  public Map<String, List<DataAlertDefinition>> getLaneToDataRuleMap() {
    return laneToDataRuleMap;
  }
}
