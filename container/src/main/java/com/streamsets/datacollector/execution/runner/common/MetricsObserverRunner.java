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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.alerts.MetricRuleEvaluator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetricsObserverRunner {

  private RulesConfigurationChangeRequest currentChangeRequest;
  /*ProductionObserver from the ProductionPipelineRunnable will set the new change request if the metric alert
  definition changes*/
  private volatile RulesConfigurationChangeRequest newChangeRequest;

  private final MetricRegistry metrics;
  private final AlertManager alertManager;
  private final String name;
  private final String rev;
  private final boolean statsAggregationEnabled;
  private Map<String, Object> resolvedParameters;
  private long pipelineStartTime;

  public MetricsObserverRunner(
      String name,
      String rev,
      boolean statsAggregationEnabled,
      MetricRegistry metrics,
      AlertManager alertManager,
      Map<String, Object> resolvedParameters,
      Configuration configuration,
      RuntimeInfo runtimeInfo
  ) {
    this.metrics = metrics;
    this.alertManager = alertManager;
    this.name = name;
    this.rev = rev;
    this.statsAggregationEnabled = statsAggregationEnabled;
    this.resolvedParameters = resolvedParameters;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
  }

  public void setPipelineStartTime(long pipelineStartTime) {
    this.pipelineStartTime = pipelineStartTime;
  }

  public void evaluate() {
    if (statsAggregationEnabled) {
      return;
    }

    //check for changes in metric rules
    RulesConfigurationChangeRequest tempNewChangeRequest = newChangeRequest;
    if(tempNewChangeRequest != null && tempNewChangeRequest != currentChangeRequest) {
      this.currentChangeRequest = tempNewChangeRequest;
      for(String alertId : currentChangeRequest.getMetricAlertsToRemove()) {
        MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(alertId), name ,rev);
      }
    }

    if (currentChangeRequest != null) {
      List<MetricsRuleDefinition> metricsRuleDefinitions =
        currentChangeRequest.getRuleDefinitions().getMetricsRuleDefinitions();
      if (metricsRuleDefinitions != null) {
        RuleDefinitionsConfigBean ruleDefinitionsConfigBean = PipelineBeanCreator.get()
            .createRuleDefinitionsConfigBean(
                currentChangeRequest.getRuleDefinitions(),
                new ArrayList<Issue>(),
                resolvedParameters
            );
        for (MetricsRuleDefinition metricsRuleDefinition : metricsRuleDefinitions) {
          MetricRuleEvaluator metricAlertsHelper = new MetricRuleEvaluator(
              metricsRuleDefinition,
              metrics,
              alertManager,
              ruleDefinitionsConfigBean,
              pipelineStartTime
          );
          metricAlertsHelper.checkForAlerts();
        }
      }
    }
  }

  public void setRulesConfigurationChangeRequest(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    this.newChangeRequest = rulesConfigurationChangeRequest;
  }


}
