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
package com.streamsets.pipeline.stage.processor.waitForJobCompletion;

import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.ControlHubApiUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WaitForJobCompletionProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForJobCompletionProcessor.class);
  private final WaitForJobCompletionConfig conf;
  private final ClientBuilder clientBuilder = ClientBuilder.newBuilder();
  private ErrorRecordHandler errorRecordHandler;

  WaitForJobCompletionProcessor(WaitForJobCompletionConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    if (conf.tlsConfig.getSslContext() != null) {
      clientBuilder.sslContext(conf.tlsConfig.getSslContext());
    }
    if (!conf.baseUrl.endsWith("/")) {
      conf.baseUrl += "/";
    }
    return issues;
  }

  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<String> jobIdList = new ArrayList<>();
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
            List<Field> jobIdsField = taskValue.get(Constants.JOB_IDS_FIELD).getValueAsList();
            jobIdsField.forEach(j -> jobIdList.add(j.getValueAsString()));
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
      String userAuthToken = ControlHubApiUtil.getUserAuthToken(
          clientBuilder,
          conf.baseUrl,
          conf.username.get(),
          conf.password.get()
      );

      List<Map<String, Object>> jobStatusList = ControlHubApiUtil.waitForJobCompletion(
          clientBuilder,
          conf.baseUrl,
          jobIdList,
          userAuthToken,
          conf.waitTime
      );
      Map<String, Map<String, Object>> jobStatusMap = jobStatusList.stream()
          .collect(Collectors.toMap(m -> (String)m.get("jobId"), m -> m));

      // update outputOrchestratorTasks with job status
      outputOrchestratorTasks.forEach((taskName, taskField) -> {
        LinkedHashMap<String, Field> taskValue = taskField.getValueAsListMap();
        boolean taskSuccess = true;
        Field jobResultsField = taskValue.get(Constants.JOB_RESULTS_FIELD);
        if (jobResultsField != null) {
          LinkedHashMap<String, Field> jobResults = jobResultsField.getValueAsListMap();
          for (Map.Entry<String, Field> entry : jobResults.entrySet()) {
            String jobId = entry.getKey();
            Field jobStatusOutputField = entry.getValue();
            LinkedHashMap<String, Field> startOutput = jobStatusOutputField.getValueAsListMap();

            Map<String, Object> jobStatus = jobStatusMap.get(jobId);
            if (jobStatus != null) {
              String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
              String statusColor = jobStatus.containsKey("color") ? (String) jobStatus.get("color") : null;
              String errorMessage = jobStatus.containsKey("errorMessage") ? (String) jobStatus.get("errorMessage") : null;
              boolean success = ControlHubApiUtil.determineJobSuccess(status, statusColor);
              startOutput.put(Constants.FINISHED_SUCCESSFULLY_FIELD, Field.create(success));
              startOutput.put(Constants.JOB_STATUS_FIELD, Field.create(status));
              startOutput.put(Constants.JOB_STATUS_COLOR_FIELD, Field.create(statusColor));
              startOutput.put(Constants.ERROR_MESSAGE_FIELD, Field.create(errorMessage));

              MetricRegistryJson jobMetrics = ControlHubApiUtil.getJobMetrics(
                  clientBuilder,
                  conf.baseUrl,
                  jobId,
                  userAuthToken
              );
              startOutput.put(Constants.JOB_METRICS_FIELD, CommonUtil.getMetricsField(jobMetrics));

              taskSuccess &= success;
            }
          }
        }
        taskValue.put(Constants.SUCCESS_FIELD, Field.create(taskSuccess));
      });

      firstRecord.set(Constants.ORCHESTRATOR_TASKS_FIELD_PATH, Field.createListMap(outputOrchestratorTasks));
      batchMaker.addRecord(firstRecord);
    }
  }

}
