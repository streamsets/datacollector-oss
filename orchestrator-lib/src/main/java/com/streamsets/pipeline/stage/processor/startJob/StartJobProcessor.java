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
package com.streamsets.pipeline.stage.processor.startJob;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class StartJobProcessor extends SingleLaneRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobProcessor.class);
  private String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  private StartJobConfig conf;
  private String userAuthToken;

  StartJobProcessor(StartJobConfig conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (!conf.baseUrl.endsWith("/")) {
      conf.baseUrl += "/";
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) {
    try {
      getUserAuthToken();
      if (conf.resetOrigin) {
        resetOffset();
      }
      startJob();
      if (conf.runInBackground) {
        batchMaker.addRecord(record);
      } else {
        waitForJobCompletion(record, batchMaker);
      }
    } catch (Exception e) {
      getContext().toError(record, e);
    }
  }

  private void waitForJobCompletion(Record record, SingleLaneBatchMaker batchMaker) {
    ThreadUtil.sleep(conf.waitTime);
    Map<String, Object> jobStatus = getJobStatus();
    String status = jobStatus.containsKey("status") ? (String) jobStatus.get("status") : null;
    if  (status != null && !status.equalsIgnoreCase("active")) {
      batchMaker.addRecord(record);
    } else {
      waitForJobCompletion(record, batchMaker);
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

  private void resetOffset() {
    String resetOffsetUrl = conf.baseUrl + "jobrunner/rest/v1/jobs/resetOffset";
    try (Response response = ClientBuilder.newClient()
        .target(resetOffsetUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(ImmutableList.of(conf.jobId)))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format("Reset failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    }
  }

  private void startJob() {
    String jobStartUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + conf.jobId + "/start";
    try (Response response = ClientBuilder.newClient()
        .target(jobStartUrl)
        .register(new CsrfProtectionFilter("CSRF"))
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .post(Entity.json(conf.runtimeParameters))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format("Job Start failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    }
  }

  private Map<String, Object> getJobStatus() {
    String jobStatusUrl = conf.baseUrl + "jobrunner/rest/v1/job/" + conf.jobId + "/currentStatus";
    try (Response response = ClientBuilder.newClient()
        .target(jobStatusUrl)
        .request()
        .header(X_USER_AUTH_TOKEN, userAuthToken)
        .get()) {
      return (Map<String, Object>)response.readEntity(Map.class);
    }
  }

}
