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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StartJobTemplateSupplier implements Supplier<Field> {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobTemplateSupplier.class);
  private String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  private final StartJobConfig conf;
  private final String templateJobId;
  private final String runtimeParametersList;
  private final Stage.Context context;
  private ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private String userAuthToken;
  private List<String> jobInstancesIdList = new ArrayList<>();

  private List<String> successStates = ImmutableList.of(
      "INACTIVE"
  );

  private List<String> errorStates = ImmutableList.of(
      "ACTIVATION_ERROR",
      "INACTIVE_ERROR"
  );

  public StartJobTemplateSupplier(
      StartJobConfig conf,
      String templateJobId,
      String runtimeParametersList,
      Stage.Context context
  ) {
    this.conf = conf;
    this.templateJobId = templateJobId;
    this.runtimeParametersList = runtimeParametersList;
    this.context = context;
  }

  @Override
  public Field get() {
    try {
      getUserAuthToken();
      List<Map<String, Object>> jobStatusList = startJobTemplate();
      jobInstancesIdList = jobStatusList.stream().map(j -> (String)j.get("jobId")).collect(Collectors.toList());
      if (conf.runInBackground) {
        generateField(jobStatusList);
      } else {
        waitForJobCompletion();
      }
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      context.reportError(ex);
    }
    return responseField;
  }

  private void waitForJobCompletion() {
    ThreadUtil.sleep(conf.waitTime);

    Map<String, Map<String, Object>> jobStatusMap = getMultipleJobStatus();
    List<Map<String, Object>> jobStatusList = jobStatusMap.keySet()
        .stream()
        .map(jobStatusMap::get)
        .collect(Collectors.toList());

    boolean allDone = true;
    for(Map<String, Object> jobStatus: jobStatusList) {
      String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
      allDone &= (successStates.contains(status) || errorStates.contains(status));
    }

    if (allDone) {
      generateField(jobStatusList);
    } else {
      waitForJobCompletion();
    }
  }

  private void getUserAuthToken() throws StageException {
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
        throw new RuntimeException(Utils.format("DPM Login failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
      userAuthToken = response.getHeaderString(X_USER_AUTH_TOKEN);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private List<Map<String, Object>> startJobTemplate() throws IOException {
    String jobStartUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + templateJobId + "/createAndStartJobInstances";
    List<Map<String, Object>> runtimeParametersList = null;
    if (StringUtils.isNotEmpty(this.runtimeParametersList)) {
      runtimeParametersList = objectMapper.readValue(this.runtimeParametersList, List.class);
    }

    Map<String, Object> jobTemplateCreationInfo = new HashMap<>();
    jobTemplateCreationInfo.put("namePostfixType", conf.instanceNameSuffix);
    jobTemplateCreationInfo.put("paramName", conf.parameterName);
    jobTemplateCreationInfo.put("runtimeParametersList", runtimeParametersList);

    try (Response response = ClientBuilder.newClient()
        .target(jobStartUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(jobTemplateCreationInfo))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format("Job Start failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
      return (List<Map<String, Object>>)response.readEntity(List.class);
    }
  }

  private Map<String, Map<String, Object>> getMultipleJobStatus() {
    String jobStatusUrl = conf.baseUrl + "jobrunner/rest/v1/jobs/status";
    try (Response response = ClientBuilder.newClient()
        .target(jobStatusUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(jobInstancesIdList))) {
      return (Map<String, Map<String, Object>>)response.readEntity(Map.class);
    }
  }

  private void generateField(List<Map<String, Object>> jobStatusList) {
    LinkedHashMap<String, Field> jobTemplateOutput = new LinkedHashMap<>();
    boolean jobTemplateSuccess = true;
    for (Map<String, Object> jobStatus : jobStatusList) {
      String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
      String jobId = (String)jobStatus.get("jobId");
      boolean success =  (status != null && successStates.contains(status));
      LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
      startOutput.put("jobId", Field.create(jobId));
      startOutput.put("success", Field.create(success));
      startOutput.put("jobStatus", Field.create(status));
      jobTemplateOutput.put(jobId, Field.createListMap(startOutput));
      jobTemplateSuccess &= success;
    }
    jobTemplateOutput.put("jobId", Field.create(templateJobId));
    jobTemplateOutput.put("success", Field.create(jobTemplateSuccess));
    responseField = Field.createListMap(jobTemplateOutput);
  }

}
