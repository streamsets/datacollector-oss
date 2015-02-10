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
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ObserverRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ObserverRunner.class);

  private RulesConfigurationChangeRequest rulesConfigurationChangeRequest;
  private final Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap;
  private final MetricRegistry metrics;
  private final ELEvaluator elEvaluator;
  private final ELEvaluator.Variables variables;
  private final Map<String, Map<String, Object>> alertResponse;
  private final AlertManager alertManager;
  private final Configuration configuration;

  public ObserverRunner(MetricRegistry metrics, AlertManager alertManager,
                        Configuration configuration) {
    this.metrics = metrics;
    this.alertResponse = new HashMap<>();
    this.ruleToSampledRecordsMap = new HashMap<>();
    this.configuration = configuration;
    elEvaluator = new ELEvaluator();
    variables = new ELEvaluator.Variables();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
    this.alertManager = alertManager;
  }

  public void handleObserverRequest(ProductionObserveRequest productionObserveRequest) {

    Map<String, List<Record>> snapshot = productionObserveRequest.getSnapshot();
    for(Map.Entry<String, List<Record>> entry : snapshot.entrySet()) {
      String lane = entry.getKey();
      List<Record> allRecords = entry.getValue();
      List<DataRuleDefinition> dataRuleDefinitions = rulesConfigurationChangeRequest.getLaneToDataRuleMap().get(lane);
      if(dataRuleDefinitions != null) {
        List<Record> sampleRecords = getSampleRecords(dataRuleDefinitions, allRecords);
        for (DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
          DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, alertManager,
            rulesConfigurationChangeRequest.getRuleDefinitions().getEmailIds(), dataRuleDefinition, configuration);
          dataRuleEvaluator.evaluateRule(allRecords, sampleRecords, lane, ruleToSampledRecordsMap);
        }
      }
    }

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
      MetricsConfigurator.removeMeter(metrics, ruleId);
      MetricsConfigurator.removeCounter(metrics, ruleId);
    }
    for(String alertId : rulesConfigurationChangeRequest.getMetricAlertsToRemove()) {
      MetricsConfigurator.removeGauge(metrics, alertId);
    }
  }

  private List<Record> getSampleRecords(List<DataRuleDefinition> dataRuleDefinitions, List<Record> allRecords) {
    double percentage = 0;
    for(DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
      if(dataRuleDefinition.getSamplingPercentage() > percentage) {
        percentage = dataRuleDefinition.getSamplingPercentage();
      }
    }
    Collections.shuffle(allRecords);
    double numberOfRecordsToSample = Math.floor(allRecords.size() * percentage / 100);
    List<Record> sampleRecords = new ArrayList<>((int)numberOfRecordsToSample);
    for(int i = 0; i < numberOfRecordsToSample; i++) {
      sampleRecords.add(((RecordImpl) allRecords.get(i)).clone());
    }
    return sampleRecords;
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
