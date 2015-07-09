/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dataCollector.execution.alerts.AlertManager;
import com.streamsets.dataCollector.execution.alerts.MetricRuleEvaluator;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.production.RulesConfigurationChangeRequest;

import javax.inject.Inject;
import java.util.List;

public class MetricsObserverRunner {

  private RulesConfigurationChangeRequest currentChangeRequest;
  /*ProductionObserver from the ProductionPipelineRunnable will set the new change request if the metric alert
  definition changes*/
  private volatile RulesConfigurationChangeRequest newChangeRequest;

  private final MetricRegistry metrics;
  private final AlertManager alertManager;
  private final String name;
  private final String rev;

  @Inject
  public MetricsObserverRunner(String name, String rev, MetricRegistry metrics, AlertManager alertManager) {
    this.metrics = metrics;
    this.alertManager = alertManager;
    this.name = name;
    this.rev = rev;
  }

  public void evaluate() {

    //check for changes in metric rules
    RulesConfigurationChangeRequest tempNewChangeRequest = newChangeRequest;
    if(tempNewChangeRequest != null && tempNewChangeRequest != currentChangeRequest) {
      this.currentChangeRequest = tempNewChangeRequest;
      for(String alertId : currentChangeRequest.getMetricAlertsToRemove()) {
        MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(alertId), name ,rev);
      }
    }

    List<MetricsRuleDefinition> metricsRuleDefinitions =
      currentChangeRequest.getRuleDefinitions().getMetricsRuleDefinitions();
    if(metricsRuleDefinitions != null) {
      for (MetricsRuleDefinition metricsRuleDefinition : metricsRuleDefinitions) {
        MetricRuleEvaluator metricAlertsHelper = new MetricRuleEvaluator(metricsRuleDefinition, metrics, alertManager,
          currentChangeRequest.getRuleDefinitions().getEmailIds());
        metricAlertsHelper.checkForAlerts();
      }
    }
  }

  public void setRulesConfigurationChangeRequest(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    this.newChangeRequest = rulesConfigurationChangeRequest;
  }


}
