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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MetricAggregationProcessor extends SingleLaneProcessor {

  // metric rule id to latest definition map
  private final Map<String, RuleDefinition> ruleDefinitionMap;
  private final String pipelineConfigJson;
  private final String ruleDefinitionsJson;
  private final String pipelineUrl;
  private final String targetUrl;
  private final String authToken;
  private final String appComponentId;
  private final String jobId;
  private final int retryAttempts;
  private final int alertTextsToRetain;

  // Metadata obtained from pipeline configuration
  private Map<String, Object> metadata;

  private MetricRegistryJson metricRegistryJson;
  private MetricRegistry metrics;
  private DataRuleHandler dataRuleHandler;
  private MetricRuleHandler metricRuleHandler;

  public MetricAggregationProcessor(
      String pipelineConfigJson,
      String ruleDefinitionsJson,
      String pipelineUrl,
      String targetUrl,
      String authToken,
      String appComponentId,
      String jobId,
      int retryAttempts,
      int alertTextsToRetain
  ) {
    ruleDefinitionMap = new HashMap<>();
    this.pipelineConfigJson = pipelineConfigJson;
    this.ruleDefinitionsJson = ruleDefinitionsJson;
    this.pipelineUrl = pipelineUrl;
    this.targetUrl = targetUrl;
    this.authToken = authToken;
    this.appComponentId = appComponentId;
    this.jobId = jobId;
    this.retryAttempts = retryAttempts;
    this.alertTextsToRetain = alertTextsToRetain;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();
    metrics = new MetricRegistry();

    PipelineConfiguration pipelineConfiguration = ConfigHelper.readPipelineConfig(
        pipelineConfigJson,
        getContext(),
        issues
    );
    RuleDefinitionsJson ruleDefJson = ConfigHelper.readRulesDefinition(
        ruleDefinitionsJson,
        getContext(),
        issues
    );

    if (issues.isEmpty()) {
      metadata = pipelineConfiguration.getMetadata();
      metricRegistryJson = getLatestAggregatedMetrics(pipelineConfiguration, ruleDefJson, issues);
    }

    if (issues.isEmpty()) {
      dataRuleHandler = new DataRuleHandler(
          getContext(),
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_ID),
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_VERSION),
          pipelineUrl,
          pipelineConfiguration,
          metrics,
          metricRegistryJson,
          ruleDefinitionMap,
          alertTextsToRetain
      );
      metricRuleHandler = new MetricRuleHandler(
          getContext(),
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_ID),
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_VERSION),
          pipelineUrl,
          metrics,
          metricRegistryJson,
          pipelineConfiguration,
          ruleDefinitionMap
      );
    }
    return issues;
  }

  @Override
  public void destroy() {
    metrics = null;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();
    if (it.hasNext()) {
      while (it.hasNext()) {
        Record record = it.next();
        switch (record.getHeader().getSourceId()) {
          case AggregatorUtil.METRIC_RULE_DISABLED:
            metricRuleHandler.handleRuleDisabledRecord(record);
            break;
          case AggregatorUtil.DATA_RULE_DISABLED:
            dataRuleHandler.handleRuleDisabledRecord(record);
            break;
          case AggregatorUtil.METRIC_RULE_CHANGE:
            metricRuleHandler.handleRuleChangeRecord(record);
            break;
          case AggregatorUtil.DATA_RULE_CHANGE:
            dataRuleHandler.handleRuleChangeRecord(record);
            break;
          case AggregatorUtil.METRIC_RULE_RECORD:
            metricRuleHandler.handleMetricRuleRecord(record);
            break;
          case AggregatorUtil.DATA_RULE_RECORD:
            dataRuleHandler.handleDataRuleRecord(record);
            break;
          case AggregatorUtil.CONFIGURATION_CHANGE:
            metricRuleHandler.handleConfigChangeRecord(record);
            dataRuleHandler.handleConfigChangeRecord(record);
            break;
          default:
            throw new IllegalArgumentException(
              Utils.format(
                "Unknown record source id '{}'",
                record.getHeader().getSourceId()
              )
            );
        }
      }
      // for each metric rule, evaluate it against the counters/meters
      evaluateRules();
      // create a new record that needs to be persisted
      publishMetricsRecord(batchMaker);
    }
  }

  private void publishMetricsRecord(SingleLaneBatchMaker batchMaker) {
    Record metricJsonRecord = null;
    try {
      String metricJsonString = ObjectMapperFactory.get().writeValueAsString(metrics);
      metricJsonRecord = createMetricJsonRecord(
          MetricAggregationConstants.AGGREGATOR,
          metadata,
          true,
          metricJsonString
      );
      batchMaker.addRecord(metricJsonRecord);
    } catch (JsonProcessingException e) {
      getContext().toError(metricJsonRecord, e);
    }
  }

  private Record createMetricJsonRecord(
      String sdcId,
      Map<String, Object> metadata,
      boolean isAggregated,
      String metricJsonString
  ) {
    Record record = getContext().createRecord("MetricAggregationProcessor");
    Map<String, Field> map = new HashMap<>();
    map.put(AggregatorUtil.TIMESTAMP, Field.create(System.currentTimeMillis()));
    map.put(AggregatorUtil.SDC_ID, Field.create(sdcId));
    map.put(AggregatorUtil.IS_AGGREGATED, Field.create(isAggregated));
    map.put(AggregatorUtil.METADATA, AggregatorUtil.getMetadataField(metadata));
    map.put(AggregatorUtil.METRIC_JSON_STRING, Field.create(metricJsonString));
    record.set(Field.create(map));
    return record;
  }

  @VisibleForTesting
  Map<String, RuleDefinition> getRuleDefinitionMap() {
    return ruleDefinitionMap;
  }

  @VisibleForTesting
  MetricRegistry getMetrics() {
    return metrics;
  }

  private void evaluateRules() {
    for(Map.Entry<String, RuleDefinition> e : ruleDefinitionMap.entrySet()) {
      RuleDefinition ruleDefinition = e.getValue();
      if (ruleDefinition instanceof MetricsRuleDefinition) {
        metricRuleHandler.evaluateMetricRules((MetricsRuleDefinition)e.getValue());
      } else if (ruleDefinition instanceof DataRuleDefinition) {
        dataRuleHandler.evaluateDataRules((DataRuleDefinition) e.getValue());
      }
    }
  }

  private MetricRegistryJson getLatestAggregatedMetrics(
    PipelineConfiguration pipelineConfiguration,
    RuleDefinitionsJson ruleDefJson,
    List<ConfigIssue> issues
  ) {
    MetricRegistryJson metricRegistryJson = null;
    if (targetUrl != null && !targetUrl.isEmpty()) {
      // fetch latest aggregated metrics from dpm time series app and reset state
      AggregatedMetricsFetcher aggregatedMetricsFetcher = new AggregatedMetricsFetcher(
          getContext(),
          targetUrl,
          authToken,
          appComponentId,
          jobId,
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_ID),
          (String)metadata.get(MetricAggregationConstants.METADATA_DPM_PIPELINE_VERSION),
          retryAttempts
      );
      metricRegistryJson = aggregatedMetricsFetcher.fetchLatestAggregatedMetrics(
        pipelineConfiguration,
        ruleDefJson,
        issues
      );
    }
    return metricRegistryJson;
  }

}
