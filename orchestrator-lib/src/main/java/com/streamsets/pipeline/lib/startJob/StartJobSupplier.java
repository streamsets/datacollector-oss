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
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StartJobSupplier implements Supplier<Field> {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobSupplier.class);
  private String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  private final StartJobConfig conf;
  private final JobIdConfig jobIdConfig;
  private final ErrorRecordHandler errorRecordHandler;
  private ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private String userAuthToken;

  private List<String> successStates = ImmutableList.of(
      "INACTIVE"
  );

  private List<String> errorStates = ImmutableList.of(
      "ACTIVATION_ERROR",
      "INACTIVE_ERROR"
  );

  public StartJobSupplier(
      StartJobConfig conf,
      JobIdConfig jobIdConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.conf = conf;
    this.jobIdConfig = jobIdConfig;
    this.errorRecordHandler = errorRecordHandler;
  }

  @Override
  public Field get() {
    try {
      getUserAuthToken();
      if (conf.resetOrigin) {
        resetOffset();
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

  private void waitForJobCompletion() {
    ThreadUtil.sleep(conf.waitTime);
    Map<String, Object> jobStatus = getJobStatus();
    String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
    if (status != null && successStates.contains(status)) {
      generateField(jobStatus);
    } else if (status != null && errorStates.contains(status)) {
      generateField(jobStatus);
    } else {
      waitForJobCompletion();
    }
  }

  private void getUserAuthToken() {
    // 1. Login to DPM to get user auth token
    Response response = null;
    try {
      Map<String, String> loginJson = new HashMap<>();
      loginJson.put("userName", conf.username.get());
      loginJson.put("password", conf.password.get());
      response = ClientBuilder.newClient()
          .target(conf.baseUrl + "security/public-rest/v1/authentication/login")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .post(Entity.json(loginJson));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_01,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
      userAuthToken = response.getHeaderString(X_USER_AUTH_TOKEN);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private void resetOffset() {
    String resetOffsetUrl = conf.baseUrl + "jobrunner/rest/v1/jobs/resetOffset";
    try (Response response = ClientBuilder.newClient()
        .target(resetOffsetUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(ImmutableList.of(jobIdConfig.jobId)))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_02,
            jobIdConfig.jobId,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
    }
  }

  private Map<String, Object> startJob() {
    String jobStartUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + jobIdConfig.jobId + "/start";
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
    try (Response response = ClientBuilder.newClient()
        .target(jobStartUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
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

  private Map<String, Object> getJobStatus() {
    String jobStatusUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + jobIdConfig.jobId + "/currentStatus";
    try (Response response = ClientBuilder.newClient()
        .target(jobStatusUrl)
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .get()) {
      return (Map<String, Object>)response.readEntity(Map.class);
    }
  }

  private void generateField(Map<String, Object> jobStatus) {
    String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
    boolean success =  (status != null && successStates.contains(status));
    LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
    startOutput.put("jobId", Field.create(jobIdConfig.jobId));
    startOutput.put("success", Field.create(success));
    startOutput.put("jobStatus", Field.create(status));
    responseField = Field.createListMap(startOutput);
  }

}
