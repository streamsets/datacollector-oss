/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.alerts.DataRuleEvaluator;
import com.streamsets.pipeline.alerts.MetricRuleEvaluator;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ObserverRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ObserverRunner.class);
  private static final String USER_PREFIX = "user.";

  private RulesConfigurationChangeRequest rulesConfigurationChangeRequest;
  private final Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap;
  private final MetricRegistry metrics;
  private final ELEvaluator elEvaluator;
  private final ELVariables variables;
  private final Map<String, Map<String, Object>> alertResponse;
  private final AlertManager alertManager;
  private final Configuration configuration;

  public ObserverRunner(MetricRegistry metrics, AlertManager alertManager,
                        Configuration configuration) {
    this.metrics = metrics;
    this.alertResponse = new HashMap<>();
    this.ruleToSampledRecordsMap = new HashMap<>();
    this.configuration = configuration;
    elEvaluator = new ELEvaluator("ObserverRunner", RecordEL.class, StringEL.class);
    variables = new ELVariables();
    this.alertManager = alertManager;
  }

  public void handleDataRulesEvaluationRequest(DataRulesEvaluationRequest dataRulesEvaluationRequest) {

    //This is the map of ruleId vs sampled records
    Map<String, Map<String, List<Record>>> snapshot = dataRulesEvaluationRequest.getSnapshot();
    for(Map.Entry<String, Map<String, List<Record>>> e : snapshot.entrySet()) {
      String lane = e.getKey();
      Map<String, List<Record>> ruleIdToSampledRecords = e.getValue();
      List<DataRuleDefinition> dataRuleDefinitions = rulesConfigurationChangeRequest.getLaneToDataRuleMap().get(lane);
      if (dataRuleDefinitions != null) {
        for (DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
          DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, alertManager,
            rulesConfigurationChangeRequest.getRuleDefinitions().getEmailIds(), dataRuleDefinition, configuration);
          dataRuleEvaluator.evaluateRule(ruleIdToSampledRecords.get(dataRuleDefinition.getId()), lane,
            ruleToSampledRecordsMap);
        }
      }
    }
  }

  public void handleMetricRulesEvaluationRequest(MetricRulesEvaluationRequest metricRulesEvaluationRequest) {
    //pipeline metric alerts
    List<MetricsRuleDefinition> metricsRuleDefinitions =
      rulesConfigurationChangeRequest.getRuleDefinitions().getMetricsRuleDefinitions();
    if(metricsRuleDefinitions != null) {
      for (MetricsRuleDefinition metricsRuleDefinition : metricsRuleDefinitions) {
        MetricRuleEvaluator metricAlertsHelper = new MetricRuleEvaluator(metricsRuleDefinition, metrics,
          variables, elEvaluator, alertManager, rulesConfigurationChangeRequest.getRuleDefinitions().getEmailIds());
        metricAlertsHelper.checkForAlerts();
      }
    }
  }

  public void handleConfigurationChangeRequest(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    //update config changes
    this.rulesConfigurationChangeRequest = rulesConfigurationChangeRequest;

    //remove metrics for changed / deleted rules
    for(String ruleId : rulesConfigurationChangeRequest.getRulesToRemove()) {
      MetricsConfigurator.removeMeter(metrics, USER_PREFIX + ruleId);
      MetricsConfigurator.removeCounter(metrics, USER_PREFIX + ruleId);
    }
    for(String alertId : rulesConfigurationChangeRequest.getMetricAlertsToRemove()) {
      MetricsConfigurator.removeGauge(metrics, alertId);
    }

    //resize evicting queue which retains sampled records
    for(Map.Entry<String, Integer> e :
      rulesConfigurationChangeRequest.getRulesWithSampledRecordSizeChanges().entrySet()) {
      if(ruleToSampledRecordsMap.get(e.getKey()) != null) {
        EvictingQueue<Record> records = ruleToSampledRecordsMap.get(e.getKey());
        int newSize = e.getValue();

        int maxSize = configuration.get(
          com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY,
          com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT);
        if(newSize > maxSize) {
          newSize = maxSize;
        }

        EvictingQueue<Record> newQueue = EvictingQueue.create(newSize);
        //this will retain only the last 'newSize' number of elements
        newQueue.addAll(records);
        ruleToSampledRecordsMap.put(e.getKey(), newQueue);
      }
    }
  }

  public List<Record> getSampledRecords(String ruleId, int size) {
    if(ruleToSampledRecordsMap.get(ruleId) != null) {
      if(ruleToSampledRecordsMap.get(ruleId).size() > size) {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId));
      }
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  RulesConfigurationChangeRequest getRulesConfigurationChangeRequest() {
    return this.rulesConfigurationChangeRequest;
  }

}
