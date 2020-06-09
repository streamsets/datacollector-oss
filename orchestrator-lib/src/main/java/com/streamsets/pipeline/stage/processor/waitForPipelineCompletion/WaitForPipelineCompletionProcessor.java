/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.waitForPipelineCompletion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.startPipeline.Groups;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineCommon;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineErrors;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WaitForPipelineCompletionProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForPipelineCompletionProcessor.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final WaitForPipelineCompletionConfig conf;
  private ErrorRecordHandler errorRecordHandler;
  public ManagerApi managerApi;
  public StoreApi storeApi;

  WaitForPipelineCompletionProcessor(WaitForPipelineCompletionConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if (conf.tlsConfig.isEnabled()) {
      conf.tlsConfig.init(
          getContext(),
          Groups.TLS.name(),
          StartPipelineCommon.SSL_CONFIG_PREFIX,
          issues
      );
    }
    // Validate Server is reachable
    if (issues.size() == 0) {
      try {
        ApiClient apiClient = StartPipelineCommon.getApiClient(
            conf.controlHubEnabled,
            conf.baseUrl,
            conf.username,
            conf.password,
            conf.controlHubUrl,
            conf.tlsConfig
        );
        SystemApi systemApi = new SystemApi(apiClient);
        systemApi.getServerTime();
        managerApi = new ManagerApi(apiClient);
        storeApi = new StoreApi(apiClient);
      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
        issues.add(
            getContext().createConfigIssue(
                Groups.PIPELINE.getLabel(),
                "conf.baseUrl",
                StartPipelineErrors.START_PIPELINE_01,
                ex.getMessage(),
                ex
            )
        );
      }
    }

    return issues;
  }

  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<String> pipelineIdList = new ArrayList<>();
    LinkedHashMap<String, Field> outputOrchestratorTasks = null;
    Record firstRecord = null;
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();
      if (firstRecord == null) {
        firstRecord = record;
      }
      try {
        Field orchestratorTasksField = record.get(Constants.ORCHESTRATOR_TASKS_FIELD_PATH);
        if (orchestratorTasksField != null) {
          LinkedHashMap<String, Field> orchestratorTasks = orchestratorTasksField.getValueAsListMap();
          orchestratorTasks.forEach((taskName, taskField) -> {
            LinkedHashMap<String, Field> taskValue = taskField.getValueAsListMap();
            List<Field> pipelineIdsField = taskValue.get(Constants.PIPELINE_IDS_FIELD).getValueAsList();
            pipelineIdsField.forEach(j -> pipelineIdList.add(j.getValueAsString()));
          });

          if (outputOrchestratorTasks == null) {
            outputOrchestratorTasks = orchestratorTasks;
          } else {
            // merge 2 orchestratorTasksField
            outputOrchestratorTasks.putAll(orchestratorTasks);
          }
        }
      } catch (OnRecordErrorException ex) {
        LOG.error(ex.toString(), ex);
        errorRecordHandler.onError(ex);
      }
    }

    if (firstRecord != null && outputOrchestratorTasks != null) {
      try {
        Map<String, PipelineStateJson> pipelineStatusMap = waitForPipelinesCompletion(pipelineIdList);

        // update outputOrchestratorTasks with job status
        outputOrchestratorTasks.forEach((taskName, taskField) -> {
          LinkedHashMap<String, Field> taskValue = taskField.getValueAsListMap();
          boolean taskSuccess = true;
          Field pipelineResultsField = taskValue.get(Constants.PIPELINE_RESULTS_FIELD);
          if (pipelineResultsField != null) {
            LinkedHashMap<String, Field> pipelineResults = pipelineResultsField.getValueAsListMap();
            for (Map.Entry<String, Field> entry : pipelineResults.entrySet()) {
              String pipelineId = entry.getKey();
              Field pipelineStatusOutputField = entry.getValue();
              LinkedHashMap<String, Field> startOutput = pipelineStatusOutputField.getValueAsListMap();

              PipelineStateJson pipelineStateJson = pipelineStatusMap.get(pipelineId);
              if (pipelineStateJson != null) {
                boolean success = Constants.PIPELINE_SUCCESS_STATES.contains(pipelineStateJson.getStatus());
                    startOutput.put(
                    Constants.FINISHED_SUCCESSFULLY_FIELD,
                    Field.create(success)
                );
                startOutput.put(Constants.PIPELINE_STATUS_FIELD, Field.create(pipelineStateJson.getStatus().toString()));
                startOutput.put(Constants.PIPELINE_STATUS_MESSAGE_FIELD, Field.create(pipelineStateJson.getMessage()));
                MetricRegistryJson jobMetrics = StartPipelineCommon.getPipelineMetrics(objectMapper, pipelineStateJson);
                startOutput.put(Constants.PIPELINE_METRICS_FIELD, CommonUtil.getMetricsField(jobMetrics));
                taskSuccess &= success;
              }
            }
          }
          taskValue.put(Constants.SUCCESS_FIELD, Field.create(taskSuccess));
        });

        firstRecord.set(Constants.ORCHESTRATOR_TASKS_FIELD_PATH, Field.createListMap(outputOrchestratorTasks));
        batchMaker.addRecord(firstRecord);
      } catch (Exception ex) {
        LOG.error(ex.toString(), ex);
        errorRecordHandler.onError(StartPipelineErrors.START_PIPELINE_06, ex.toString(), ex);
      }
    }
  }

  private Map<String, PipelineStateJson> waitForPipelinesCompletion(List<String> pipelineIdList) throws Exception {
    boolean allDone = true;
    Map<String, PipelineStateJson> statusMap = new HashMap<>();
    for (String pipelineId: pipelineIdList) {
      PipelineStateJson pipelineStateJson = managerApi.getPipelineStatus(pipelineId, "0");
      statusMap.put(pipelineId, pipelineStateJson);
      if (!Constants.PIPELINE_SUCCESS_STATES.contains(pipelineStateJson.getStatus()) &&
          !Constants.PIPELINE_ERROR_STATES.contains(pipelineStateJson.getStatus())) {
        allDone = false;
      }
    }
    if (!allDone) {
      ThreadUtil.sleep(conf.waitTime);
      return waitForPipelinesCompletion(pipelineIdList);
    }
    return statusMap;
  }

}
