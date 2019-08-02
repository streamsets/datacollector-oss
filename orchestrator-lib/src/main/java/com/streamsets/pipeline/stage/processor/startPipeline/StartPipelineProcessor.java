/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.startPipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.datacollector.client.model.SourceOffsetJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;

public class StartPipelineProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineProcessor.class);
  private static final String REV = "0";
  private StartPipelineConfig conf;
  private ManagerApi managerApi;
  private ObjectMapper objectMapper = new ObjectMapper();

  private List<PipelineStateJson.StatusEnum> successStates = ImmutableList.of(
      PipelineStateJson.StatusEnum.STOPPED,
      PipelineStateJson.StatusEnum.FINISHED
  );

  private List<PipelineStateJson.StatusEnum> errorStates = ImmutableList.of(
      PipelineStateJson.StatusEnum.START_ERROR,
      PipelineStateJson.StatusEnum.RUN_ERROR,
      PipelineStateJson.StatusEnum.STOP_ERROR,
      PipelineStateJson.StatusEnum.DISCONNECTED
  );

  StartPipelineProcessor(StartPipelineConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    ApiClient apiClient;

    // Validate Server is reachable
    try {
      apiClient = getApiClient();
      SystemApi systemApi = new SystemApi(apiClient);
      systemApi.getServerTime();
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.PIPELINE.getLabel(),
              "conf.baseUrl",
              Errors.START_PIPELINE_01,
              ex.getMessage(),
              ex
          )
      );
      return issues;
    }

    // Validate pipelineId exists in the server
    try {
      StoreApi storeApi = new StoreApi(apiClient);
      storeApi.getPipelineInfo(conf.pipelineId, REV, "info", false);
    } catch (ApiException ex) {
      LOG.error(ex.getMessage(), ex);
      issues.add(
          getContext().createConfigIssue(
              Groups.PIPELINE.getLabel(),
              "conf.pipelineId",
              Errors.START_PIPELINE_02,
              ex.getMessage(),
              ex
          )
      );
      return issues;
    }

    managerApi = new ManagerApi(apiClient);
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) {
    try {
      if (conf.resetOrigin) {
        managerApi.resetOffset(conf.pipelineId, REV);
      }
      PipelineStateJson pipelineStateJson = managerApi.startPipeline(conf.pipelineId, REV, conf.runtimeParameters);
      if (conf.runInBackground) {
        updateRecord(record, pipelineStateJson);
        batchMaker.addRecord(record);
      } else {
        waitForPipelineCompletion(record, batchMaker);
      }
    } catch (Exception e) {
      getContext().toError(record, e);
    }
  }

  private void waitForPipelineCompletion(Record record, SingleLaneBatchMaker batchMaker) throws Exception {
    PipelineStateJson pipelineStateJson = managerApi.getPipelineStatus(conf.pipelineId, REV);
    if (successStates.contains(pipelineStateJson.getStatus())) {
      updateRecord(record, pipelineStateJson);
      batchMaker.addRecord(record);
    } else if (errorStates.contains(pipelineStateJson.getStatus())) {
      updateRecord(record, pipelineStateJson);
      getContext().toError(record, pipelineStateJson.getMessage());
    } else {
      ThreadUtil.sleep(conf.waitTime);
      waitForPipelineCompletion(record, batchMaker);
    }
  }

  private void updateRecord(Record record, PipelineStateJson pipelineStateJson) throws Exception {
    // after done add status, offset and metrics to record
    SourceOffsetJson sourceOffset = managerApi.getCommittedOffsets(conf.pipelineId, REV);
    LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
    startOutput.put("committedOffsets", Field.create(objectMapper.writeValueAsString(sourceOffset)));
    startOutput.put("pipelineState", Field.create(objectMapper.writeValueAsString(pipelineStateJson)));
    record.set(conf.outputFieldPath, Field.createListMap(startOutput));
  }

  private ApiClient getApiClient() throws StageException {
    String authType = "form";
    if (conf.controlHubEnabled) {
      authType = "dpm";
    }
    ApiClient apiClient = new ApiClient(authType);
    apiClient.setUserAgent("Start Pipeline Processor");
    apiClient.setBasePath(conf.baseUrl + "/rest");
    apiClient.setUsername(conf.username.get());
    apiClient.setPassword(conf.password.get());
    apiClient.setDPMBaseURL(conf.controlHubUrl);
    return  apiClient;
  }
}
