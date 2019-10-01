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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.execution.alerts.AlertManagerHelper;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataRuleHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DataRuleHandler.class);

  // Data rule metrics
  private final Map<String, Counter> evaluatedRecordCounterMap;
  private Map<String, Counter> matchedRecordCounterMap;
  private Map<String, BoundedDeque<String>> ruleToAlertTextForMatchedRecords;
  private Map<String, List<String>> stageToOutputLanesMap;
  private final MetricRegistryJson metricRegistryJson;
  private final MetricRegistry metrics;
  private final String pipelineName;
  private final String revision;
  // metric rule id to latest definition map
  private final Map<String, RuleDefinition> ruleDefinitionMap;
  private final RulesEvaluator rulesEvaluator;
  private final int alertTextsToRetain;

  public DataRuleHandler(
      Stage.Context context,
      String pipelineName,
      String revision,
      String pipelineUrl,
      PipelineConfiguration pipelineConfiguration,
      MetricRegistry metrics,
      MetricRegistryJson metricRegistryJson,
      Map<String, RuleDefinition> ruleDefinitionMap,
      int alertTextsToRetain
  ) {
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.ruleDefinitionMap = ruleDefinitionMap;
    this.matchedRecordCounterMap = new HashMap<>();
    this.ruleToAlertTextForMatchedRecords = new HashMap<>();
    this.metricRegistryJson = metricRegistryJson;
    this.metrics = metrics;
    this.evaluatedRecordCounterMap = new HashMap<>();
    this.stageToOutputLanesMap = new HashMap<>();
    for (StageConfiguration s : pipelineConfiguration.getStages()) {
      stageToOutputLanesMap.put(s.getInstanceName(), s.getOutputAndEventLanes());
    }
    this.rulesEvaluator = new RulesEvaluator(pipelineName, revision, pipelineUrl, context);
    this.alertTextsToRetain = alertTextsToRetain;
  }

  void handleDataRuleRecord(Record record) {

    String ruleId = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULE_ID).getValueAsString();
    long evaluatedRecordCount = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.EVALUATED_RECORDS).getValueAsInteger();
    long matchedRecordCount = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.MATCHED_RECORDS).getValueAsInteger();

    List<Field> alertTextFields = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.ALERT_TEXTS).getValueAsList();
    List<String> alertTexts = new ArrayList<>();
    for(Field f : alertTextFields) {
      alertTexts.add(f.getValueAsString());
    }
    // For both Data and Drift Rule, We need to cache alert text only if its not the same as the previous alert text
    // In addition We will store only the last n alert texts
    BoundedDeque<String> boundedDeque = ruleToAlertTextForMatchedRecords.get(ruleId);
    if (null == boundedDeque) {
      boundedDeque = new BoundedDeque<>(alertTextsToRetain);
      ruleToAlertTextForMatchedRecords.put(ruleId, boundedDeque);
    }
    for (String alertText : alertTexts) {
      boundedDeque.offerLast(alertText);
    }

    if (RulesHelper.isDataRuleRecordValid(ruleDefinitionMap, record)) {
      Counter evaluatedRecordCounter = evaluatedRecordCounterMap.get(ruleId);
      if (evaluatedRecordCounter == null) {
        evaluatedRecordCounter = MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          getEvaluatedCounterName(ruleId),
          pipelineName,
          revision
        );
        evaluatedRecordCounterMap.put(ruleId, evaluatedRecordCounter);
      }
      evaluatedRecordCounter.inc(evaluatedRecordCount);
      Counter matchingRecordCounter = matchedRecordCounterMap.get(ruleId);
      if (matchingRecordCounter == null) {
        matchingRecordCounter = MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          getMatchedCounterName(ruleId),
          pipelineName,
          revision
        );
        matchedRecordCounterMap.put(ruleId, matchingRecordCounter);
      }
      matchingRecordCounter.inc(matchedRecordCount);
      AlertManagerHelper.updateDataRuleMeter(
          metrics,
          (DataRuleDefinition)ruleDefinitionMap.get(ruleId),
          matchedRecordCount,
          pipelineName,
          revision
      );
    }

  }

  void handleRuleChangeRecord(Record record) {
    if (RulesHelper.isRuleDefinitionLatest(ruleDefinitionMap, record)) {
      RuleDefinition rDef = AggregatorUtil.getDataRuleDefinition(record);
      // remove data rule counters associated with the rule
      removeDataRuleMetrics(rDef.getId());
      // remove gauge associated with rule
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(rDef.getId()), pipelineName, revision);
      ruleDefinitionMap.put(rDef.getId(), rDef);
    }
  }

  void handleRuleDisabledRecord(Record record) {
    if (RulesHelper.isRuleDefinitionLatest(ruleDefinitionMap, record)) {
      // remove alertDataRule gauge if present
      String ruleId = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULE_ID).getValueAsString();
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleId), pipelineName, revision);
      ruleDefinitionMap.remove(ruleId);

      // remove counters
      if (AggregatorUtil.DATA_RULE_DISABLED.equals(record.getHeader().getSourceId())) {
        removeDataRuleMetrics(ruleId);
      }
    }
  }

  void handleConfigChangeRecord(Record record) {

    List<Field> valueAsList = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.EMAILS).getValueAsList();
    List<String> emails = new ArrayList<>();
    for (Field f : valueAsList) {
      emails.add(f.getValueAsString());
    }
    rulesEvaluator.setEmails(emails);

    Map<String, Field> rulesToRemove = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULES_TO_REMOVE).getValueAsListMap();
    for (String ruleId : rulesToRemove.keySet()) {
      removeDataRuleMetrics(ruleId);
    }

    List<Field> alertsToRemove = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.METRIC_ALERTS_TO_REMOVE).getValueAsList();
    for (Field f : alertsToRemove) {
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(f.getValueAsString()), pipelineName, revision);
    }

  }

  void evaluateDataRules(DataRuleDefinition dataRuleDefinition) {
    if (dataRuleDefinition.isEnabled()) {
      double threshold;
      try {
        threshold = Double.parseDouble(dataRuleDefinition.getThresholdValue());
      } catch (NumberFormatException e) {
        //Soft error for now as we don't want this alertDataRule to stop other rules
        LOG.error("Error interpreting threshold '{}' as a number", dataRuleDefinition.getThresholdValue(), e);
        return;
      }

      Counter matchingRecordCounter = matchedRecordCounterMap.get(dataRuleDefinition.getId());
      if (matchingRecordCounter != null) {
        switch (dataRuleDefinition.getThresholdType()) {
          case COUNT:
            if (matchingRecordCounter.getCount() > threshold) {
              if (dataRuleDefinition instanceof DriftRuleDefinition ||
                dataRuleDefinition instanceof DataRuleDefinition) {
                rulesEvaluator.alert(
                    metrics,
                    dataRuleDefinition,
                    matchingRecordCounter.getCount(),
                    ruleToAlertTextForMatchedRecords
                );
              } else {
                throw new RuntimeException(Utils.format(
                  "Unexpected RuleDefinition class '{}'",
                  dataRuleDefinition.getClass().getName()
                ));
              }
            }
            break;
          case PERCENTAGE:
            Counter evaluatedRecordCounter = evaluatedRecordCounterMap.get(dataRuleDefinition.getId());
            if ((matchingRecordCounter.getCount() * 100.0 / evaluatedRecordCounter.getCount()) > threshold
              && evaluatedRecordCounter.getCount() >= dataRuleDefinition.getMinVolume()) {
              rulesEvaluator.alert(
                  metrics,
                  dataRuleDefinition,
                  matchingRecordCounter.getCount(),
                  ruleToAlertTextForMatchedRecords
              );
            }
            break;
          default:
            throw new IllegalArgumentException(
              Utils.format(
                "Unknown Data Rule Threshold Type '{}'",
                dataRuleDefinition.getThresholdType()
              )
            );
        }
      }
    }
  }

  private void removeDataRuleMetrics(String ruleId) {
    evaluatedRecordCounterMap.remove(ruleId);
    matchedRecordCounterMap.remove(ruleId);
    MetricsConfigurator.removeCounter(metrics, getEvaluatedCounterName(ruleId), pipelineName, revision);
    if (metricRegistryJson != null) {
      metricRegistryJson.getCounters().remove(getEvaluatedCounterName(ruleId) + MetricsConfigurator.COUNTER_SUFFIX);
    }
    MetricsConfigurator.removeCounter(metrics, getMatchedCounterName(ruleId), pipelineName, revision);
    if (metricRegistryJson != null) {
      metricRegistryJson.getCounters().remove(getMatchedCounterName(ruleId) + MetricsConfigurator.COUNTER_SUFFIX);
    }
  }

  private String getMatchedCounterName(String ruleId) {
    return MetricAggregationConstants.USER_PREFIX + ruleId + ".matched";
  }

  private String getEvaluatedCounterName(String ruleId) {
    return MetricAggregationConstants.USER_PREFIX + ruleId + ".evaluated";
  }

}
