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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.alerts.AlertManager;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.runner.production.DataRulesEvaluationRequest;
import com.streamsets.datacollector.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataObserverRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataObserverRunner.class);
  private static final String USER_PREFIX = "user.";

  private RulesConfigurationChangeRequest rulesConfigurationChangeRequest;
  private final Map<String, EvictingQueue<SampledRecord>> ruleToSampledRecordsMap;
  private final MetricRegistry metrics;
  private final AlertManager alertManager;
  private final Configuration configuration;
  private final String name;
  private final String rev;
  private MetricRegistryJson metricRegistryJson;
  private final Map<String, Object> pipelineELContext;
  private BlockingQueue<Record> startsAggregatorQueue;
  private Map<String, Object> resolvedParameters;

  DataObserverRunner(
      String name,
      String rev,
      MetricRegistry metrics,
      AlertManager alertManager,
      Configuration configuration,
      Map<String, Object> resolvedParameters
  ) {
    this.metrics = metrics;
    this.ruleToSampledRecordsMap = new HashMap<>();
    this.configuration = configuration;
    this.alertManager = alertManager;
    this.name = name;
    this.rev = rev;
    this.pipelineELContext = new HashMap<>();
    this.resolvedParameters = resolvedParameters;
  }

  void setStatsQueue(BlockingQueue<Record> startsAggregatorQueue) {
    this.startsAggregatorQueue = startsAggregatorQueue;
  }

  void handleDataRulesEvaluationRequest(DataRulesEvaluationRequest dataRulesEvaluationRequest) {

    //This is the map of ruleId vs sampled records
    Map<String, Map<String, List<Record>>> snapshot = dataRulesEvaluationRequest.getSnapshot();
    for(Map.Entry<String, Map<String, List<Record>>> e : snapshot.entrySet()) {
      String lane = e.getKey();
      Map<String, List<Record>> ruleIdToSampledRecords = e.getValue();
      List<DataRuleDefinition> dataRuleDefinitions = rulesConfigurationChangeRequest.getLaneToDataRuleMap().get(lane);

      if (dataRuleDefinitions != null) {
        for (DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
          //sampled records for that rule
          List<Record> sampledRecords = ruleIdToSampledRecords.get(dataRuleDefinition.getId());
          if(dataRuleDefinition.isEnabled()  && sampledRecords != null && sampledRecords.size() > 0) {
            //evaluate rule only if it is enabled and there are sampled records.
            RuleDefinitionsConfigBean ruleDefinitionsConfigBean = PipelineBeanCreator.get()
                .createRuleDefinitionsConfigBean(
                    rulesConfigurationChangeRequest.getRuleDefinitions(),
                    new ArrayList<Issue>(),
                    resolvedParameters
                );
            DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(
                name,
                rev,
                metrics,
                alertManager,
                ruleDefinitionsConfigBean,
                pipelineELContext,
                dataRuleDefinition,
                configuration,
                metricRegistryJson,
                startsAggregatorQueue
            );
            dataRuleEvaluator.evaluateRule(sampledRecords, lane, ruleToSampledRecordsMap);
          } else if (!dataRuleDefinition.isEnabled()) {
            //If data rule is disabled, clear the sampled records for that rule
            EvictingQueue<SampledRecord> records = ruleToSampledRecordsMap.get(dataRuleDefinition.getId());
            if(records != null) {
              records.clear();
            }
          }
        }
      }
    }
  }

  public void handleConfigurationChangeRequest(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    //update config changes
    this.rulesConfigurationChangeRequest = rulesConfigurationChangeRequest;

    //remove metrics for changed / deleted rules
    for(String ruleId : rulesConfigurationChangeRequest.getRulesToRemove().keySet()) {
      MetricsConfigurator.removeMeter(metrics, USER_PREFIX + ruleId, name, rev);
      MetricsConfigurator.removeCounter(metrics, USER_PREFIX + ruleId, name, rev);
      EvictingQueue<SampledRecord> records = ruleToSampledRecordsMap.get(ruleId);
      if(records != null) {
        records.clear();
      }
    }

    // send RulesConfigurationChangeRequest to stats aggregator
    if (startsAggregatorQueue != null) {
      startsAggregatorQueue.offer(
          AggregatorUtil.createConfigChangeRequestRecord(
              rulesConfigurationChangeRequest,
              resolvedParameters
          )
      );
    }

    //resize evicting queue which retains sampled records
    for(Map.Entry<String, Integer> e :
      rulesConfigurationChangeRequest.getRulesWithSampledRecordSizeChanges().entrySet()) {
      if(ruleToSampledRecordsMap.get(e.getKey()) != null) {
        EvictingQueue<SampledRecord> records = ruleToSampledRecordsMap.get(e.getKey());
        int newSize = e.getValue();

        int maxSize = configuration.get(
          Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY,
          Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT);
        if(newSize > maxSize) {
          newSize = maxSize;
        }

        EvictingQueue<SampledRecord> newQueue = EvictingQueue.create(newSize);
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
      RuleDefinitionsConfigBean ruleDefinitionsConfigBean = PipelineBeanCreator.get()
          .createRuleDefinitionsConfigBean(
              rulesConfigurationChangeRequest.getRuleDefinitions(),
              new ArrayList<Issue>(),
              resolvedParameters
          );
      List<String> emailIds = ruleDefinitionsConfigBean.emailIDs;
      if (emailIds != null && !emailIds.isEmpty()) {
        alertManager.alert(emailIds, request.getThrowable());
      }
    }
  }

  public List<SampledRecord> getSampledRecords(String ruleId, int size) {
    if(ruleToSampledRecordsMap.get(ruleId) != null) {
      if(ruleToSampledRecordsMap.get(ruleId).size() > size) {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId));
      }
    }
    return Collections.emptyList();
  }

  @VisibleForTesting // make package private after refactoring
  public RulesConfigurationChangeRequest getRulesConfigurationChangeRequest() {
    return this.rulesConfigurationChangeRequest;
  }

  public void setMetricRegistryJson(MetricRegistryJson metricRegistryJson) {
    this.metricRegistryJson = metricRegistryJson;
  }

}
