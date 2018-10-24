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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricRuleEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRuleEvaluator.class);

  private final MetricsRuleDefinition metricsRuleDefinition;
  private final MetricRegistry metrics;
  private final RuleDefinitionsConfigBean ruleDefinitionsConfigBean;
  private final AlertManager alertManager;
  private final long pipelineStartTime;

  public MetricRuleEvaluator(
      MetricsRuleDefinition metricsRuleDefinition,
      MetricRegistry metricRegistry,
      AlertManager alertManager,
      RuleDefinitionsConfigBean ruleDefinitionsConfigBean,
      long pipelineStartTime
  ) {
    this.metricsRuleDefinition = metricsRuleDefinition;
    this.metrics = metricRegistry;
    this.ruleDefinitionsConfigBean = ruleDefinitionsConfigBean;
    this.alertManager = alertManager;
    this.pipelineStartTime = pipelineStartTime;
  }

  public void checkForAlerts() {
    if (metricsRuleDefinition.isEnabled()) {
      try {
        Object value = MetricRuleEvaluatorHelper.getMetricValue(
          metrics,
          metricsRuleDefinition.getMetricId(),
          metricsRuleDefinition.getMetricType(),
          metricsRuleDefinition.getMetricElement()
        );

        if(value != null) {
          if (MetricRuleEvaluatorHelper.evaluate(pipelineStartTime, value, metricsRuleDefinition.getCondition())) {
            alertManager.alert(value, ruleDefinitionsConfigBean, metricsRuleDefinition);
          }
        }
      } catch (ObserverException e) {
        //A faulty condition should not take down rest of the alerts with it.
        //Log and it and continue for now
        LOG.error("Error processing metric definition alert '{}', reason: {}", metricsRuleDefinition.getId(),
          e.toString(), e);
        //Trigger alert with exception message
        alertManager.alertException(e.toString(), metricsRuleDefinition);
      }
    }
  }

}
