/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.DriftRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.HistogramJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.restapi.bean.TimerJson;
import com.streamsets.datacollector.runner.LaneResolver;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatedMetricsFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsFetcher.class);

  private final Stage.Context context;
  private final String targetUrl;
  private final String authToken;
  private final String appComponentId;
  private final String pipelineId;
  private final String pipelineVersion;

  private Client client;
  private WebTarget target;

  public AggregatedMetricsFetcher(
      Stage.Context context,
      String targetUrl,
      String authToken,
      String appComponentId,
      String pipelineId,
      String pipelineVersion
  ) {
    this.context = context;
    this.targetUrl = targetUrl;
    this.authToken = authToken;
    this.appComponentId = appComponentId;
    this.pipelineId = pipelineId;
    this.pipelineVersion = pipelineVersion;
  }

  public MetricRegistryJson fetchLatestAggregatedMetrics(
      PipelineConfigurationJson pipelineConfigurationJson,
      RuleDefinitionsJson ruleDefJson,
      List<Stage.ConfigIssue> issues
  ) {
    // fetch last persisted metrics for the following counters, timers, meters and histograms
    MetricRegistryJson metricRegistryJson = buildMetricRegistryJson(pipelineConfigurationJson, ruleDefJson);

    client = ClientBuilder.newBuilder().build();
    client.register(new CsrfProtectionFilter("CSRF"));
    client.register(GZipEncoder.class);
    target = client.target(targetUrl);

    Response response = target
      .queryParam("pipelineId", pipelineId)
      .queryParam("pipelineVersion", pipelineVersion)
      .request()
      .header("X-Requested-By", MetricAggregationConstants.SDC)
      .header("X-SS-App-Auth-Token", authToken.replaceAll("(\\r|\\n)", ""))
      .header("X-SS-App-Component-Id", appComponentId)
      .post(Entity.json(metricRegistryJson));

    if (response.getStatus() != 200) {
      String responseMessage = response.readEntity(String.class);
      LOG.error(Utils.format(Errors.STATS_02.getMessage(), responseMessage));
      issues.add(
          context.createConfigIssue(
            Groups.STATS.getLabel(),
            "targetUrl",
            Errors.STATS_02,
            responseMessage
          )
      );
      metricRegistryJson = null;
    } else {
      metricRegistryJson = response.readEntity(MetricRegistryJson.class);
    }
    client.close();
    return metricRegistryJson;
  }

  private MetricRegistryJson buildMetricRegistryJson(
    PipelineConfigurationJson pipelineConfigurationJson,
    RuleDefinitionsJson ruleDefJson
  ) {

    Map<String, TimerJson> timerJsonMap = new HashMap<>();
    Map<String, CounterJson> countersJsonMap = new HashMap<>();
    Map<String, MeterJson> meterJsonMap = new HashMap<>();
    Map<String, HistogramJson> histogramJsonMap = new HashMap<>();
    Map<String, Object> gauges = new HashMap<>();

    timerJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_PROCESSING + MetricsConfigurator.TIMER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_COUNT + MetricsConfigurator.COUNTER_SUFFIX, null);

    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_COUNT + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_INPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_OUTPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_MESSAGES + MetricsConfigurator.METER_SUFFIX, null);

    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_INPUT_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_OUTPUT_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_ERROR_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_ERRORS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);

    for (StageConfigurationJson s : pipelineConfigurationJson.getStages()) {
      String stageInstanceName = s.getInstanceName();
      String metricsKey = MetricAggregationConstants.STAGE_PREFIX + stageInstanceName;

      timerJsonMap.put( metricsKey + MetricAggregationConstants.BATCH_PROCESSING + MetricsConfigurator.TIMER_SUFFIX, null);

      meterJsonMap.put(metricsKey + MetricAggregationConstants.INPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.ERROR_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.STAGE_ERRORS + MetricsConfigurator.METER_SUFFIX, null);

      histogramJsonMap.put(metricsKey + MetricAggregationConstants.INPUT_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.ERROR_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.STAGE_ERRORS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);

      // create counter and meter for output lanes
      for (String lane : s.getOutputLanes()) {
        meterJsonMap.put(metricsKey + ":" + lane + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
        countersJsonMap.put(metricsKey + ":" + lane + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
      }
    }

    // for rules
    if (ruleDefJson != null) {
      for (DataRuleDefinitionJson d : ruleDefJson.getDataRuleDefinitions()) {
        gauges.put(AlertsUtil.getAlertGaugeName(d.getId()) + MetricsConfigurator.GAUGE_SUFFIX, null);
        countersJsonMap.put(MetricAggregationConstants.USER_PREFIX + d.getId() + MetricsConfigurator.COUNTER_SUFFIX, null);
        countersJsonMap.put(LaneResolver.getPostFixedLaneForObserver(d.getLane()), null);
      }
      for (MetricsRuleDefinitionJson d : ruleDefJson.getMetricsRuleDefinitions()) {
        gauges.put(AlertsUtil.getAlertGaugeName(d.getId()) + MetricsConfigurator.GAUGE_SUFFIX, null);
      }
      for (DriftRuleDefinitionJson d : ruleDefJson.getDriftRuleDefinitions()) {
        gauges.put(AlertsUtil.getAlertGaugeName(d.getId()) + MetricsConfigurator.GAUGE_SUFFIX, null);
        countersJsonMap.put(MetricAggregationConstants.USER_PREFIX + d.getId() + MetricsConfigurator.COUNTER_SUFFIX, null);
        countersJsonMap.put(LaneResolver.getPostFixedLaneForObserver(d.getLane()), null);
      }
    }

    MetricRegistryJson metricRegistryJson = new MetricRegistryJson();
    metricRegistryJson.setTimers(timerJsonMap);
    metricRegistryJson.setCounters(countersJsonMap);
    metricRegistryJson.setMeters(meterJsonMap);
    metricRegistryJson.setHistograms(histogramJsonMap);
    metricRegistryJson.setGauges(gauges);

    return metricRegistryJson;
  }
}
