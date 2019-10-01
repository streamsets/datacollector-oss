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

import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.CounterJson;
import com.streamsets.datacollector.restapi.bean.DataRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.DriftRuleDefinitionJson;
import com.streamsets.datacollector.event.json.HistogramJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.event.json.TimerJson;
import com.streamsets.datacollector.runner.LaneResolver;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.util.StatsUtil;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AggregatedMetricsFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsFetcher.class);

  private final Stage.Context context;
  private final String targetUrl;
  private final String authToken;
  private final String appComponentId;
  private final String jobId;
  private final String pipelineId;
  private final String pipelineVersion;
  private final int retryAttempts;

  private Client client;
  private WebTarget target;

  public AggregatedMetricsFetcher(
      Stage.Context context,
      String targetUrl,
      String authToken,
      String appComponentId,
      String jobId,
      String pipelineId,
      String pipelineVersion,
      int retryAttempts
  ) {
    this.context = context;
    this.targetUrl = targetUrl;
    this.authToken = authToken;
    this.appComponentId = appComponentId;
    this.jobId = jobId;
    this.pipelineId = pipelineId;
    this.pipelineVersion = pipelineVersion;
    this.retryAttempts = retryAttempts;
  }

  public MetricRegistryJson fetchLatestAggregatedMetrics(
      PipelineConfiguration pipelineConfiguration,
      RuleDefinitionsJson ruleDefJson,
      List<Stage.ConfigIssue> issues
  ) {
    // fetch last persisted metrics for the following counters, timers, meters and histograms
    MetricRegistryJson metricRegistryJson = buildMetricRegistryJson(pipelineConfiguration, ruleDefJson);

    client = ClientBuilder.newBuilder().build();
    client.register(new CsrfProtectionFilter("CSRF"));
    client.register(GZipEncoder.class);
    target = client.target(targetUrl);
    Entity<MetricRegistryJson> metricRegistryJsonEntity = Entity.json(metricRegistryJson);
    String errorResponseMessage = null;
    int delaySecs = 1;
    int attempts = 0;

    // Wait for a few random seconds before starting the stopwatch
    int waitSeconds = new Random().nextInt(60);
    try {
      Thread.sleep(waitSeconds * 1000);
    } catch (InterruptedException e) {
      // No-op
    }

    while (attempts < retryAttempts || retryAttempts == -1) {
      if (attempts > 0) {
        delaySecs = delaySecs * 2;
        delaySecs = Math.min(delaySecs, 60);
        LOG.warn("DPM fetchLatestAggregatedMetrics attempt '{}', waiting for '{}' seconds before retrying ...",
            attempts, delaySecs);
        StatsUtil.sleep(delaySecs);
      }
      attempts++;

      Response response = null;
      try {
        response = target
            .queryParam("jobId", jobId)
            .queryParam("pipelineId", pipelineId)
            .queryParam("pipelineVersion", pipelineVersion)
            .request()
            .header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME)
            .header(SSOConstants.X_APP_AUTH_TOKEN, authToken.replaceAll("(\\r|\\n)", ""))
            .header(SSOConstants.X_APP_COMPONENT_ID, appComponentId)
            .post(metricRegistryJsonEntity);

        if (response.getStatus() == HttpURLConnection.HTTP_OK) {
          metricRegistryJson = response.readEntity(MetricRegistryJson.class);
          break;
        } else if (response.getStatus() == HttpURLConnection.HTTP_UNAVAILABLE) {
          errorResponseMessage = "Error requesting latest stats from time-series app: DPM unavailable";
          LOG.warn(errorResponseMessage);
          metricRegistryJson = null;
        }  else if (response.getStatus() == HttpURLConnection.HTTP_FORBIDDEN) {
          errorResponseMessage = response.readEntity(String.class);
          LOG.error(Utils.format(Errors.STATS_02.getMessage(), errorResponseMessage));
          metricRegistryJson = null;
          break;
        } else {
          errorResponseMessage = response.readEntity(String.class);
          LOG.warn("Error requesting latest stats from time-series app, HTTP status '{}': {}",
              response.getStatus(), errorResponseMessage);
          metricRegistryJson = null;
        }
      } catch (Exception ex) {
        errorResponseMessage = ex.toString();
        LOG.warn("Error requesting latest stats from time-series app : {}", ex.toString());
        metricRegistryJson = null;
      }  finally {
        if (response != null) {
          response.close();
        }
      }
    }

    if (metricRegistryJson == null) {
      issues.add(
          context.createConfigIssue(
              Groups.STATS.getLabel(),
              "targetUrl",
              Errors.STATS_02,
              errorResponseMessage
          )
      );
    }

    return metricRegistryJson;
  }

  private MetricRegistryJson buildMetricRegistryJson(
    PipelineConfiguration pipelineConfiguration,
    RuleDefinitionsJson ruleDefJson
  ) {

    Map<String, TimerJson> timerJsonMap = new HashMap<>();
    Map<String, CounterJson> countersJsonMap = new HashMap<>();
    Map<String, MeterJson> meterJsonMap = new HashMap<>();
    Map<String, HistogramJson> histogramJsonMap = new HashMap<>();
    Map<String, Object> gauges = new HashMap<>();

    timerJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_PROCESSING + MetricsConfigurator.TIMER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_COUNT + MetricsConfigurator.COUNTER_SUFFIX, null);

    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_COUNT + MetricsConfigurator.COUNTER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_INPUT_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_OUTPUT_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
    countersJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_MESSAGES + MetricsConfigurator.COUNTER_SUFFIX, null);

    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_COUNT + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_INPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_OUTPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
    meterJsonMap.put(MetricAggregationConstants.PIPELINE_BATCH_ERROR_MESSAGES + MetricsConfigurator.METER_SUFFIX, null);

    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_INPUT_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_OUTPUT_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_ERROR_RECORDS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
    histogramJsonMap.put(MetricAggregationConstants.PIPELINE_ERRORS_PER_BATCH + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);

    for (StageConfiguration s : pipelineConfiguration.getStages()) {
      String stageInstanceName = s.getInstanceName();
      String metricsKey = MetricAggregationConstants.STAGE_PREFIX + stageInstanceName;

      timerJsonMap.put( metricsKey + MetricAggregationConstants.BATCH_PROCESSING + MetricsConfigurator.TIMER_SUFFIX, null);

      countersJsonMap.put(metricsKey + MetricAggregationConstants.INPUT_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
      countersJsonMap.put(metricsKey + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
      countersJsonMap.put(metricsKey + MetricAggregationConstants.ERROR_RECORDS + MetricsConfigurator.COUNTER_SUFFIX, null);
      countersJsonMap.put(metricsKey + MetricAggregationConstants.STAGE_ERRORS + MetricsConfigurator.COUNTER_SUFFIX, null);

      meterJsonMap.put(metricsKey + MetricAggregationConstants.INPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.ERROR_RECORDS + MetricsConfigurator.METER_SUFFIX, null);
      meterJsonMap.put(metricsKey + MetricAggregationConstants.STAGE_ERRORS + MetricsConfigurator.METER_SUFFIX, null);

      histogramJsonMap.put(metricsKey + MetricAggregationConstants.INPUT_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.OUTPUT_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.ERROR_RECORDS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);
      histogramJsonMap.put(metricsKey + MetricAggregationConstants.STAGE_ERRORS + MetricsConfigurator.HISTOGRAM_M5_SUFFIX, null);

      // create counter and meter for output lanes
      for (String lane : s.getOutputAndEventLanes()) {
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
