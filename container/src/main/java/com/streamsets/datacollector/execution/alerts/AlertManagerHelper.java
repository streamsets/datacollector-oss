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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.metrics.MetricsConfigurator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertManagerHelper {

  private static final String USER_PREFIX = "user.";

  @SuppressWarnings("unchecked")
  public static void alertException(
    String pipelineName,
    String revision,
    MetricRegistry metrics,
    Object value,
    RuleDefinition ruleDefinition
  ) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(EmailConstants.EXCEPTION_MESSAGE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));
    if (gauge != null) {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()), pipelineName,
        revision);
    }
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge, pipelineName, revision);
  }

  @SuppressWarnings("unchecked")
  public static void updateAlertGauge(Gauge gauge, Object value, RuleDefinition ruleDefinition) {
    Map<String, Object> alertResponse = (Map<String, Object>) gauge.getValue();
    // we keep timestamp of first trigger
    // update current value
    alertResponse.put(EmailConstants.CURRENT_VALUE, value);
    // add new alert text
    List<String> alertTexts = (List<String>) alertResponse.get(EmailConstants.ALERT_TEXTS);
    alertTexts = new ArrayList<>(alertTexts);
    updateAlertText(ruleDefinition, alertTexts);
    alertResponse.put(EmailConstants.ALERT_TEXTS, alertTexts);
  }

  private static void updateAlertText(RuleDefinition ruleDefinition, List<String> alertTexts) {
    // As of today Data and Metric Rules have the same fixed alert texts for a given rule.
    // But Drift rules can have different alert texts based on the drift in the values.

    // The check below will avoid collecting the same alert texts again and again in case of metric and data rules.

    // One may ask why not just add alert texts the first time for metric and data rules and skip updating it?
    // Because when we add EL support in alert texts then metric and data rules may produce different alert texts.

    int size = alertTexts.size();
    String alertText = ruleDefinition.getAlertText();
    if (size > 0) {
      String lastAlertText = alertTexts.get(size - 1);
      if (!lastAlertText.equals(alertText)) {
        alertTexts.add(alertText);
        if (alertTexts.size() > 10) {
          alertTexts.remove(0);
        }
      }
    } else {
      // add
      alertTexts.add(alertText);
      if (alertTexts.size() > 10) {
        alertTexts.remove(0);
      }
    }
  }

  public static Gauge<Object> createAlertResponseGauge(
    String pipelineName,
    String revision,
    MetricRegistry metrics,
    Object value,
    RuleDefinition ruleDefinition) {
    final Map<String, Object> alertResponse = new HashMap<>();
    alertResponse.put(EmailConstants.CURRENT_VALUE, value);
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    alertResponse.put(EmailConstants.TIMESTAMP, System.currentTimeMillis());
    List<String> alertTexts = new ArrayList<>();
    alertTexts.add(ruleDefinition.getAlertText());
    alertResponse.put(EmailConstants.ALERT_TEXTS, alertTexts);

    MetricsConfigurator.createGauge(metrics, AlertsUtil.getAlertGaugeName(ruleDefinition.getId()),
      alertResponseGauge, pipelineName, revision);
    return alertResponseGauge;
  }

  public static DataRuleDefinition cloneRuleWithResolvedAlertText(DataRuleDefinition def, String alterInfo) {
    return new DataRuleDefinition(
      def.getFamily(),
      def.getId(),
      def.getLabel(),
      def.getLane(),
      def.getSamplingPercentage(),
      def.getSamplingRecordsToRetain(),
      def.getCondition(),
      def.isAlertEnabled(),
      alterInfo,
      def.getThresholdType(),
      def.getThresholdValue(),
      def.getMinVolume(),
      def.isMeterEnabled(),
      def.isSendEmail(),
      def.isAlertEnabled(),
      def.getTimestamp()
    );
  }

  public static void updateDataRuleMeter(
    MetricRegistry metrics,
    DataRuleDefinition dataRuleDefinition,
    long matchingCount,
    String pipelineName,
    String revision
  ) {
    if (dataRuleDefinition.isMeterEnabled() && matchingCount > 0) {
      Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
      if (meter == null) {
        meter = MetricsConfigurator.createMeter(
          metrics,
          USER_PREFIX + dataRuleDefinition.getId(),
          pipelineName,
          revision
        );
      }
      meter.mark(matchingCount);
    }
  }
}
