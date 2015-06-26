/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.dataCollector.execution.alerts.AlertManager;
import com.streamsets.dataCollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.prodmanager.Constants;
import com.streamsets.pipeline.runner.production.DataRulesEvaluationRequest;
import com.streamsets.pipeline.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.pipeline.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataObserverRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataObserverRunner.class);
  private static final String USER_PREFIX = "user.";

  private RulesConfigurationChangeRequest rulesConfigurationChangeRequest;
  private final Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap;
  private final MetricRegistry metrics;
  private final AlertManager alertManager;
  private final Configuration configuration;

  public DataObserverRunner(MetricRegistry metrics, AlertManager alertManager,
                            Configuration configuration) {
    this.metrics = metrics;
    this.ruleToSampledRecordsMap = new HashMap<>();
    this.configuration = configuration;
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
          DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, alertManager,
            rulesConfigurationChangeRequest.getRuleDefinitions().getEmailIds(), dataRuleDefinition, configuration);
          dataRuleEvaluator.evaluateRule(ruleIdToSampledRecords.get(dataRuleDefinition.getId()), lane,
            ruleToSampledRecordsMap);
        }
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

    //resize evicting queue which retains sampled records
    for(Map.Entry<String, Integer> e :
      rulesConfigurationChangeRequest.getRulesWithSampledRecordSizeChanges().entrySet()) {
      if(ruleToSampledRecordsMap.get(e.getKey()) != null) {
        EvictingQueue<Record> records = ruleToSampledRecordsMap.get(e.getKey());
        int newSize = e.getValue();

        int maxSize = configuration.get(
          Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY,
          Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT);
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

  public void handlePipelineErrorNotificationRequest(PipelineErrorNotificationRequest request) {
    RulesConfigurationChangeRequest rulesConfigurationChangeRequest = this.rulesConfigurationChangeRequest;
    if (rulesConfigurationChangeRequest == null) {
      LOG.error("Cannot send alert for throwable due to null RulesConfigurationChangeRequest: " +
        request.getThrowable(), request.getThrowable());
    } else {
      List<String> emailIds = rulesConfigurationChangeRequest.getRuleDefinitions().getEmailIds();
      if (emailIds != null && !emailIds.isEmpty()) {
        alertManager.alert(emailIds, request.getThrowable());
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
