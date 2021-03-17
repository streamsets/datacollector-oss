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
package com.streamsets.pipeline.lib.startJob;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.ControlHubApiUtil;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StartJobSupplier implements Supplier<Field> {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobSupplier.class);
  private final StartJobConfig conf;
  private final JobIdConfig jobIdConfig;
  private final ErrorRecordHandler errorRecordHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private final ClientBuilder clientBuilder;

  public StartJobSupplier(
      StartJobConfig conf,
      JobIdConfig jobIdConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.conf = conf;
    this.jobIdConfig = jobIdConfig;
    this.errorRecordHandler = errorRecordHandler;
    clientBuilder = conf.controlHubConfig.getClientBuilder();
  }

  @Override
  public Field get() {
    try {
      if (jobIdConfig.jobIdType.equals(JobIdType.NAME)) {
        // fetch Job ID using GET jobs REST API using query param filterText=<job name>
        initializeJobId();
      }
      if (conf.resetOrigin) {
        ControlHubApiUtil.resetOffset(clientBuilder, conf.controlHubConfig.baseUrl, jobIdConfig.jobId);
      }
      Map<String, Object> jobStatus = startJob();
      if (conf.runInBackground) {
        generateField(jobStatus);
      } else {
        waitForJobCompletion();
      }
    } catch (StageException ex) {
      LOG.error(ex.getMessage(), ex);
      errorRecordHandler.onError(ex.getErrorCode(), ex.getMessage(), ex);
    }
    return responseField;
  }

  /*
    Doing a self-referential call to enter in a wait loop can potentially create a
    StackOverfkowException, and could provoke an unestable JVM due to OutOfMemmoryException.
   */
  private void waitForJobCompletion() {
    while(true) {
      ThreadUtil.sleep(conf.waitTime);
      Map<String, Object> jobStatus = ControlHubApiUtil.getJobStatus(clientBuilder,
          conf.controlHubConfig.baseUrl,
          jobIdConfig.jobId
      );
      String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
      if (status != null &&
          (Constants.JOB_SUCCESS_STATES.contains(status) ||
           Constants.JOB_ERROR_STATES.contains(status))) {
        generateField(jobStatus);
        return;
      }
    }
  }

  private void initializeJobId() {
    String fetchJobsUrl = conf.controlHubConfig.baseUrl + "jobrunner/rest/v1/jobs";
    try (Response response = clientBuilder.build()
        .target(fetchJobsUrl)
        .queryParam("filterText", jobIdConfig.jobId)
        .request()
        .get()) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_02,
            jobIdConfig.jobId,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }

      List<Map<String, Object>> jobs = (List<Map<String, Object>>)response.readEntity(List.class);
      if (jobs.size() != 1) {
        throw new StageException(
            StartJobErrors.START_JOB_07,
            jobIdConfig.jobId,
            jobs.size()
        );
      }
      jobIdConfig.jobId = (String)jobs.get(0).get("id");
    }
  }

  private Map<String, Object> startJob() {
    String jobStartUrl = conf.controlHubConfig.baseUrl + "jobrunner/rest/v1/job/" + jobIdConfig.jobId + "/start";
    Map<String, Object> runtimeParameters = null;
    if (StringUtils.isNotEmpty(jobIdConfig.runtimeParameters)) {
      try {
        runtimeParameters = objectMapper.readValue(jobIdConfig.runtimeParameters, Map.class);
      } catch (IOException e) {
        throw new StageException(
            StartJobErrors.START_JOB_05,
            jobIdConfig.jobId,
            e.toString(),
            e
        );
      }
    }
    try (Response response = clientBuilder.build()
        .target(jobStartUrl)
        .request()
        .post(Entity.json(runtimeParameters))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_03,
            jobIdConfig.jobId,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
      return (Map<String, Object>)response.readEntity(Map.class);
    }
  }

  private void generateField(Map<String, Object> jobStatus) {
    String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
    String statusColor = jobStatus.containsKey("color") ? (String) jobStatus.get("color") : null;
    String errorMessage = jobStatus.containsKey("errorMessage") ? (String) jobStatus.get("errorMessage") : null;
    boolean success = ControlHubApiUtil.determineJobSuccess(status, statusColor);
    LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
    startOutput.put(Constants.JOB_ID_FIELD, Field.create(jobIdConfig.jobId));
    startOutput.put(Constants.STARTED_SUCCESSFULLY_FIELD, Field.create(true));
    if (!conf.runInBackground) {
      startOutput.put(Constants.FINISHED_SUCCESSFULLY_FIELD, Field.create(success));
      MetricRegistryJson jobMetrics = ControlHubApiUtil.getJobMetrics(
          clientBuilder,
          conf.controlHubConfig.baseUrl,
          jobIdConfig.jobId
      );
      startOutput.put(Constants.JOB_METRICS_FIELD, CommonUtil.getMetricsField(jobMetrics));
    }
    startOutput.put(Constants.JOB_STATUS_FIELD, Field.create(status));
    startOutput.put(Constants.JOB_STATUS_COLOR_FIELD, Field.create(statusColor));
    startOutput.put(Constants.ERROR_MESSAGE_FIELD, Field.create(errorMessage));
    responseField = Field.createListMap(startOutput);
  }

}
