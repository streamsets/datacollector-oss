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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetricRuleEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRuleEvaluator.class);

  private final MetricsRuleDefinition metricsRuleDefinition;
  private final MetricRegistry metrics;
  private final List<String> emailIds;
  private final AlertManager alertManager;

  public MetricRuleEvaluator(MetricsRuleDefinition metricsRuleDefinition, MetricRegistry metricRegistry,
                             AlertManager alertManager, List<String> emailIds) {
    this.metricsRuleDefinition = metricsRuleDefinition;
    this.metrics = metricRegistry;
    this.emailIds = emailIds;
    this.alertManager = alertManager;
  }

  public void checkForAlerts() {
    if (metricsRuleDefinition.isEnabled()) {
      Metric metric = MetricRuleEvaluatorHelper.getMetric(
        metrics,
        metricsRuleDefinition.getMetricId(),
        metricsRuleDefinition.getMetricType()
      );
      if(metric != null) {
        try {
          Object value = MetricRuleEvaluatorHelper.getMetricValue(
            metricsRuleDefinition.getMetricElement(),
            metricsRuleDefinition.getMetricType(),
            metric
          );
          if (MetricRuleEvaluatorHelper.evaluate(value, metricsRuleDefinition.getCondition())) {
            alertManager.alert(value, emailIds, metricsRuleDefinition);
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

}
