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
package com.streamsets.pipeline.lib;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.client.model.PipelineStateJson;

import java.util.List;

public class Constants {
  public static final String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  public static final String ORCHESTRATOR_TASKS_FIELD_NAME = "orchestratorTasks";
  public static final String ORCHESTRATOR_TASKS_FIELD_PATH = "/orchestratorTasks";
  public static final String JOB_ID_FIELD = "jobId";
  public static final String JOB_IDS_FIELD = "jobIds";
  public static final String JOB_RESULTS_FIELD = "jobResults";
  public static final String JOB_STATUS_FIELD = "jobStatus";
  public static final String JOB_STATUS_COLOR_FIELD = "jobStatusColor";
  public static final String JOB_METRICS_FIELD = "jobMetrics";

  public static final String PIPELINE_ID_FIELD = "pipelineId";
  public static final String PIPELINE_TITLE_FIELD = "pipelineTitle";
  public static final String PIPELINE_IDS_FIELD = "pipelineIds";
  public static final String PIPELINE_RESULTS_FIELD = "pipelineResults";
  public static final String PIPELINE_STATUS_FIELD = "pipelineStatus";
  public static final String PIPELINE_STATUS_MESSAGE_FIELD = "pipelineStatusMessage";
  public static final String PIPELINE_METRICS_FIELD = "pipelineMetrics";
  public static final String COMMITTED_OFFSET_STR = "committedOffsetsStr";

  public static final String TEMPLATE_JOB_INSTANCES_FIELD = "templateJobInstances";
  public static final String TEMPLATE_JOB_ID_FIELD = "templateJobId";
  public static final String STARTED_SUCCESSFULLY_FIELD = "startedSuccessfully";
  public static final String FINISHED_SUCCESSFULLY_FIELD = "finishedSuccessfully";
  public static final String ERROR_MESSAGE_FIELD = "errorMessage";
  public static final String SUCCESS_FIELD = "success";


  public static final List<String> JOB_SUCCESS_STATES = ImmutableList.of(
      "INACTIVE"
  );
  public static final List<String> JOB_ERROR_STATES = ImmutableList.of(
      "ACTIVATION_ERROR",
      "INACTIVE_ERROR"
  );
  public static final String JOB_STATUS_COLOR_RED = "RED";

  public static final String REV = "0";
  public static final List<PipelineStateJson.StatusEnum> PIPELINE_SUCCESS_STATES = ImmutableList.of(
      PipelineStateJson.StatusEnum.STOPPED,
      PipelineStateJson.StatusEnum.FINISHED
  );
  public static final List<PipelineStateJson.StatusEnum> PIPELINE_ERROR_STATES = ImmutableList.of(
      PipelineStateJson.StatusEnum.START_ERROR,
      PipelineStateJson.StatusEnum.RUN_ERROR,
      PipelineStateJson.StatusEnum.STOP_ERROR,
      PipelineStateJson.StatusEnum.DISCONNECTED
  );

  private Constants() {}
}
