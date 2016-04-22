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
package com.streamsets.pipeline.stage.destination.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.SDCMetricsJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
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
import java.io.IOException;
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
  private static final String SDC = "sdc";
  private static final String X_REQUESTED_BY = "X-Requested-By";
  private static final String X_SS_APP_AUTH_TOKEN = "X-SS-App-Auth-Token";
  private static final String X_SS_APP_COMPONENT_ID = "X-SS-App-Component-Id";

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

  private Client client;
  private WebTarget target;
  private boolean commit;
  private Stopwatch stopwatch;
  private final Map<String, Record> sdcIdToRecordMap;

  public HttpTarget(
      String targetUrl,
      String authToken,
      String appComponentId,
      String pipelineCommitId,
      String jobId,
      int waitTimeBetweenUpdates
  ) {
    this.targetUrl = targetUrl;
    this.sdcAuthToken = authToken;
    this.sdcId = appComponentId;
    this.pipelineCommitId = pipelineCommitId;
    this.jobId = jobId;
    this.waitTimeBetweenUpdates = waitTimeBetweenUpdates;
    sdcIdToRecordMap = new LinkedHashMap<>();
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
    client.register(GZipEncoder.class);
    target = client.target(targetUrl);
    stopwatch = Stopwatch.createStarted();
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
    client.close();
  }

  private void handleException(Exception e, Record currentRecord) throws StageException {
    switch (getContext().getOnErrorRecord()) {
      case DISCARD:
        break;
      case TO_ERROR:
        getContext().toError(currentRecord, e);
        break;
      case STOP_PIPELINE:
        if (e instanceof StageException) {
          LOG.error(e.getMessage());
          throw (StageException) e;
        } else {
          LOG.error(Utils.format(Errors.HTTP_01.getMessage(), currentRecord.getHeader().getSourceId(), e.toString(), e));
          throw new StageException(Errors.HTTP_01, currentRecord.getHeader().getSourceId(), e.toString(), e);
        }
      default:
        throw new IllegalStateException(Utils.format("Unknown OnErrorRecord option '{}'",
          getContext().getOnErrorRecord()));
    }
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
      handleException(e, tempRecord);
    }
    return sdcMetricsJsonList;
  }

  private void sendUpdate(List<SDCMetricsJson> sdcMetricsJsonList) throws StageException {
    Response response = target.request()
      .header(X_REQUESTED_BY, SDC)
      .header(X_SS_APP_AUTH_TOKEN, sdcAuthToken.replaceAll("(\\r|\\n)", ""))
      .header(X_SS_APP_COMPONENT_ID, sdcId)
      .post(
        Entity.json(
          sdcMetricsJsonList
        )
      );
    if (response.getStatus() != 200) {
      String responseMessage = response.readEntity(String.class);
      LOG.error(Utils.format(Errors.HTTP_02.getMessage(), responseMessage));
      throw new StageException(Errors.HTTP_02, responseMessage);
    }
  }

  private SDCMetricsJson createSdcMetricJson(Record currentRecord) throws IOException {
    SDCMetricsJson sdcMetricsJson = new SDCMetricsJson();
    sdcMetricsJson.setSdcId(currentRecord.get("/" + AggregatorUtil.SDC_ID).getValueAsString());
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
