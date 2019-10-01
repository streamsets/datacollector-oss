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
package com.streamsets.pipeline.stage.destination.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.streamsets.datacollector.http.SnappyWriterInterceptor;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.util.StatsUtil;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpTarget extends BaseTarget implements OffsetCommitTrigger {

  private static final Logger LOG = LoggerFactory.getLogger(HttpTarget.class);

  @VisibleForTesting
  static final String DPM_PIPELINE_COMMIT_ID = "dpm.pipeline.commitId";
  @VisibleForTesting
  static final String DPM_JOB_ID = "dpm.job.id";

  private final String targetUrl;
  private final String sdcAuthToken;
  private final String sdcId;
  private final String pipelineCommitId;
  private final String jobId;
  private final int waitTimeBetweenUpdates;
  private final int retryAttempts;
  private final boolean compressRequests;
  private final Map<String, Record> sdcIdToRecordMap;

  private Client client;
  private WebTarget target;
  private boolean commit;
  private Stopwatch stopwatch;
  private ErrorRecordHandler errorRecordHandler;

  public HttpTarget(
      String targetUrl,
      String authToken,
      String appComponentId,
      String pipelineCommitId,
      String jobId,
      int waitTimeBetweenUpdates,
      boolean compressRequests,
      int retryAttempts
  ) {
    this.targetUrl = targetUrl;
    this.sdcAuthToken = authToken;
    this.sdcId = appComponentId;
    this.pipelineCommitId = pipelineCommitId;
    this.jobId = jobId;
    this.waitTimeBetweenUpdates = waitTimeBetweenUpdates;
    this.compressRequests = compressRequests;
    sdcIdToRecordMap = new LinkedHashMap<>();
    this.retryAttempts = retryAttempts;
  }

  @Override
  public void write(Batch batch) throws StageException {
    commit = false;
    // cache records using sdc Id as key
    cacheRecords(batch);
    // update target if it is time
    if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > waitTimeBetweenUpdates) {
      List<SDCMetricsJson> sdcMetricsJsonList = getRecordsToWrite();
      if (!sdcMetricsJsonList.isEmpty()) {
        // send update
        sendUpdate(sdcMetricsJsonList);
        // Need to commit offset on completion of this batch
        commit = true;
        // clear cache and reset stopwatch
        sdcIdToRecordMap.clear();
        stopwatch.reset();
        stopwatch.start();
      }
    }
  }

  @Override
  public List<ConfigIssue> init() {
    super.init();
    client = ClientBuilder.newBuilder().build();
    client.register(new CsrfProtectionFilter("CSRF"));
    if (compressRequests) {
      client.register(SnappyWriterInterceptor.class);
    }
    target = client.target(targetUrl);
    stopwatch = Stopwatch.createStarted();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
    client.close();
  }

  @Override
  public boolean commit() {
    return commit;
  }

  public List<SDCMetricsJson> getRecordsToWrite() throws StageException {
    List<SDCMetricsJson> sdcMetricsJsonList = new ArrayList<>();
    Record tempRecord = null;
    try {
      for (Record currentRecord : sdcIdToRecordMap.values()) {
        tempRecord = currentRecord;
        SDCMetricsJson sdcMetricsJson = createSdcMetricJson(currentRecord);
        sdcMetricsJsonList.add(sdcMetricsJson);
      }
    } catch (IOException e) {
      errorRecordHandler.onError(
          new OnRecordErrorException(
              tempRecord,
              Errors.HTTP_01,
              tempRecord.getHeader().getSourceId(),
              e.toString(),
              e
          )
      );
    }
    return sdcMetricsJsonList;
  }

  private void sendUpdate(List<SDCMetricsJson> sdcMetricsJsonList) throws StageException {
    int delaySecs = 1;
    int attempts = 0;
    while (attempts < retryAttempts || retryAttempts == -1) {
      if (attempts > 0) {
        delaySecs = delaySecs * 2;
        delaySecs = Math.min(delaySecs, 60);
        LOG.warn("Post attempt '{}', waiting for '{}' seconds before retrying ...",
          attempts, delaySecs);
        StatsUtil.sleep(delaySecs);
      }
      attempts++;
      Response response = null;
      try {
        response = target.request()
          .header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME)
          .header(SSOConstants.X_APP_AUTH_TOKEN, sdcAuthToken.replaceAll("(\\r|\\n)", ""))
          .header(SSOConstants.X_APP_COMPONENT_ID, sdcId)
          .post(
            Entity.json(
              sdcMetricsJsonList
            )
          );
        if (response.getStatus() == HttpURLConnection.HTTP_OK) {
          return;
        } else if (response.getStatus() == HttpURLConnection.HTTP_UNAVAILABLE) {
          LOG.warn("Error writing to time-series app: DPM unavailable");
          // retry
        } else if (response.getStatus() == HttpURLConnection.HTTP_FORBIDDEN) {
          // no retry in this case
          String errorResponseMessage = response.readEntity(String.class);
          LOG.error(Utils.format(Errors.HTTP_02.getMessage(), errorResponseMessage));
          throw new StageException(Errors.HTTP_02, errorResponseMessage);
        } else {
          String responseMessage = response.readEntity(String.class);
          LOG.error(Utils.format(Errors.HTTP_02.getMessage(), responseMessage));
          //retry
        }
      } catch (Exception ex) {
        LOG.error(Utils.format(Errors.HTTP_02.getMessage(), ex.toString(), ex));
        // retry
      } finally {
        if (response != null) {
          response.close();
        }
      }
    }
    // no success after retry
    throw new StageException(Errors.HTTP_03, retryAttempts);
  }

  private SDCMetricsJson createSdcMetricJson(Record currentRecord) throws IOException {
    SDCMetricsJson sdcMetricsJson = new SDCMetricsJson();
    sdcMetricsJson.setSdcId(currentRecord.get("/" + AggregatorUtil.SDC_ID).getValueAsString());
    Field value =  currentRecord.get("/" + AggregatorUtil.MASTER_SDC_ID);
    if (value != null) {
      sdcMetricsJson.setMasterSdcId(value.getValueAsString());
    }
    sdcMetricsJson.setTimestamp(currentRecord.get("/" + AggregatorUtil.TIMESTAMP).getValueAsLong());
    sdcMetricsJson.setAggregated(currentRecord.get("/" + AggregatorUtil.IS_AGGREGATED).getValueAsBoolean());
    Map<String, Field> valueAsListMap = currentRecord.get("/" + AggregatorUtil.METADATA).getValueAsMap();
    if (valueAsListMap != null && !valueAsListMap.isEmpty()) {
      // Metadata is not available as of now, make it mandatory once available
      Map<String, String> metadata = new HashMap<>();
      for (Map.Entry<String, Field> e : valueAsListMap.entrySet()) {
        metadata.put(e.getKey(), e.getValue().getValueAsString());
      }
      metadata.put(DPM_PIPELINE_COMMIT_ID, pipelineCommitId);
      metadata.put(DPM_JOB_ID, jobId);

      Field timeSeriesAnalysisField =  currentRecord.get("/" + AggregatorUtil.TIME_SERIES_ANALYSIS);
      if (timeSeriesAnalysisField != null) {
        metadata.put(AggregatorUtil.TIME_SERIES_ANALYSIS, timeSeriesAnalysisField.getValueAsString());
      }
      // SDC's from 3.2 will have the last record field
      Field isLastRecord = currentRecord.get("/" + AggregatorUtil.LAST_RECORD);
      if (isLastRecord != null) {
        if (Boolean.valueOf(isLastRecord.getValueAsString())) {
          LOG.info(
              "Got last metric record from {} running job {}",
              sdcMetricsJson.getSdcId(),
              jobId
          );
        }
        metadata.put(AggregatorUtil.LAST_RECORD, isLastRecord.getValueAsString());
      }
      sdcMetricsJson.setMetadata(metadata);
    }
    String metricRegistryJson = currentRecord.get("/" + AggregatorUtil.METRIC_JSON_STRING).getValueAsString();
    sdcMetricsJson.setMetrics(ObjectMapperFactory.get().readValue(metricRegistryJson, MetricRegistryJson.class));
    return sdcMetricsJson;
  }

  private void cacheRecords(Batch batch) {
    Iterator<Record> records = batch.getRecords();
    Record record;
    while(records.hasNext()) {
      record = records.next();
      sdcIdToRecordMap.put(record.get("/" + AggregatorUtil.SDC_ID).getValueAsString(), record);
    }
  }

}
