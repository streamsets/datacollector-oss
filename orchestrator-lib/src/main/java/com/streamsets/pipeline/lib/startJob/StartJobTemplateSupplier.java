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
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.ControlHubApiUtil;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
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
  private final StartJobConfig conf;
  private final String templateJobId;
  private final String runtimeParametersList;
  private final ErrorRecordHandler errorRecordHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private String userAuthToken;
  private final ClientBuilder clientBuilder = ClientBuilder.newBuilder();


  public StartJobTemplateSupplier(
      StartJobConfig conf,
      String templateJobId,
      String runtimeParametersList,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.conf = conf;
    this.templateJobId = templateJobId;
    this.runtimeParametersList = runtimeParametersList;
    this.errorRecordHandler = errorRecordHandler;

    if (conf.tlsConfig.getSslContext() != null) {
      clientBuilder.sslContext(conf.tlsConfig.getSslContext());
    }
  }

  @Override
  public Field get() {
    try {
      userAuthToken = ControlHubApiUtil
          .getUserAuthToken(clientBuilder, conf.baseUrl, conf.username.get(), conf.password.get());
      List<Map<String, Object>> jobStatusList = startJobTemplate();
      List<String> jobInstancesIdList = jobStatusList.stream()
          .map(j -> (String) j.get("jobId")).collect(Collectors.toList());
      if (!conf.runInBackground) {
        jobStatusList = ControlHubApiUtil.waitForJobCompletion(
            clientBuilder,
            conf.baseUrl,
            jobInstancesIdList,
            userAuthToken,
            conf.waitTime
        );
      }
      generateField(jobStatusList);
    } catch (StageException ex) {
      LOG.error(ex.getMessage(), ex);
      errorRecordHandler.onError(ex.getErrorCode(), ex.getMessage(), ex);
    }
    return responseField;
  }

  private List<Map<String, Object>> startJobTemplate() throws OnRecordErrorException {
    String jobStartUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + templateJobId + "/createAndStartJobInstances";
    List<Map<String, Object>> runtimeParametersList = null;
    if (StringUtils.isNotEmpty(this.runtimeParametersList)) {
      try {
        runtimeParametersList = objectMapper.readValue(this.runtimeParametersList, List.class);
      } catch (IOException e) {
        throw new StageException(
            StartJobErrors.START_JOB_05,
            templateJobId,
            e.toString(),
            e
        );
      }
    }

    Map<String, Object> jobTemplateCreationInfo = new HashMap<>();
    jobTemplateCreationInfo.put("namePostfixType", conf.instanceNameSuffix);
    jobTemplateCreationInfo.put("paramName", conf.parameterName);
    jobTemplateCreationInfo.put("runtimeParametersList", runtimeParametersList);

    try (Response response = clientBuilder.build()
        .target(jobStartUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(Constants.X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(jobTemplateCreationInfo))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_04,
            templateJobId,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
      return (List<Map<String, Object>>)response.readEntity(List.class);
    }
  }



  private void generateField(List<Map<String, Object>> jobStatusList) {
    LinkedHashMap<String, Field> jobTemplateOutput = new LinkedHashMap<>();
    List<Field> templateJobInstances = new ArrayList<>();
    boolean jobTemplateSuccess = true;
    for (Map<String, Object> jobStatus : jobStatusList) {
      String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
      String statusColor = jobStatus.containsKey("color") ? (String) jobStatus.get("color") : null;
      String errorMessage = jobStatus.containsKey("errorMessage") ? (String) jobStatus.get("errorMessage") : null;
      String jobId = (String)jobStatus.get("jobId");
      boolean success = ControlHubApiUtil.determineJobSuccess(status, statusColor);
      LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
      startOutput.put(Constants.JOB_ID_FIELD, Field.create(jobId));
      startOutput.put(Constants.STARTED_SUCCESSFULLY_FIELD, Field.create(true));
      if (!conf.runInBackground) {
        startOutput.put(Constants.FINISHED_SUCCESSFULLY_FIELD, Field.create(success));
        MetricRegistryJson jobMetrics = ControlHubApiUtil.getJobMetrics(
            clientBuilder,
            conf.baseUrl,
            jobId,
            userAuthToken
        );
        startOutput.put(Constants.JOB_METRICS_FIELD, CommonUtil.getMetricsField(jobMetrics));
      }
      startOutput.put(Constants.JOB_STATUS_FIELD, Field.create(status));
      startOutput.put(Constants.JOB_STATUS_COLOR_FIELD, Field.create(statusColor));
      startOutput.put(Constants.ERROR_MESSAGE_FIELD, Field.create(errorMessage));
      templateJobInstances.add(Field.createListMap(startOutput));
      jobTemplateSuccess &= success;
    }
    jobTemplateOutput.put(Constants.TEMPLATE_JOB_ID_FIELD, Field.create(templateJobId));
    jobTemplateOutput.put(Constants.TEMPLATE_JOB_INSTANCES_FIELD, Field.create(templateJobInstances));
    jobTemplateOutput.put(Constants.STARTED_SUCCESSFULLY_FIELD, Field.create(true));
    if (!conf.runInBackground) {
      jobTemplateOutput.put(Constants.FINISHED_SUCCESSFULLY_FIELD, Field.create(jobTemplateSuccess));
    }
    responseField = Field.createListMap(jobTemplateOutput);
  }

}
