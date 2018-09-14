/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.databricks;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.lib.http.GrizzlyClientCustomizer;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_01;
import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_02;
import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_03;
import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_04;
import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_05;
import static com.streamsets.pipeline.stage.executor.databricks.Errors.DATABRICKS_06;
import static com.streamsets.pipeline.stage.executor.databricks.JobType.JAR;
import static com.streamsets.pipeline.stage.executor.databricks.JobType.NOTEBOOK;

public class DatabricksJobExecutor extends BaseExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DatabricksJobExecutor.class);
  private final DatabricksConfigBean databricksConfigBean;
  private Client client;
  private static final String SPARK_GROUP = "SPARK";
  private static final String APPLICATION_GROUP = "APPLICATION";
  private static final String PREFIX = "conf.";

  private static final String RUN_NOW_URL = "/api/2.0/jobs/run-now";
  private static final String LIST_URL = "/api/2.0/jobs/get";
  public static final String APP_SUBMITTED_EVENT = "AppSubmittedEvent";
  public static final String APP_ID = "app-id";

  /**
   * Issued for every submitted Spark job.
   */
  public static final EventCreator JOB_CREATED = new EventCreator.Builder(APP_SUBMITTED_EVENT, 1)
      .withRequiredField(APP_ID)
      .build();

  private String baseUrl;

  public DatabricksJobExecutor(DatabricksConfigBean configs) {
    this.databricksConfigBean = configs;
  }

  @Override
  public List<Stage.ConfigIssue> init() {
    List<Stage.ConfigIssue> issues = new ArrayList<>();

    Optional
        .ofNullable(databricksConfigBean.init(getContext(), PREFIX))
        .ifPresent(issues::addAll);

    baseUrl = databricksConfigBean.baseUrl.endsWith("/") ?
        databricksConfigBean.baseUrl.substring(0, databricksConfigBean.baseUrl.length() - 1) :
        databricksConfigBean.baseUrl;

    HttpProxyConfigBean proxyConf = databricksConfigBean.proxyConfigBean;
    String proxyUsername = null;
    String proxyPassword = null;
    if(databricksConfigBean.useProxy) {
      proxyUsername = proxyConf.resolveUsername(getContext(), "PROXY", "conf.proxyConfigBean.", issues);
      proxyPassword = proxyConf.resolvePassword(getContext(), "PROXY", "conf.proxyConfigBean.", issues);
    }

    if(issues.isEmpty()) {
      ClientConfig clientConfig = new ClientConfig()
          .property(ClientProperties.ASYNC_THREADPOOL_SIZE, 1)
          .property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);

      if (databricksConfigBean.useProxy) {
        clientConfig = clientConfig.connectorProvider(new GrizzlyConnectorProvider(new GrizzlyClientCustomizer(
            true,
            proxyUsername,
            proxyPassword
        )));
      }

      ClientBuilder builder = getClientBuilder()
          .withConfig(clientConfig)
          .register(JacksonJsonProvider.class);
      HttpAuthenticationFeature auth = null;
      if (databricksConfigBean.credentialsConfigBean.credentialType == CredentialType.PASSWORD) {
        String username = databricksConfigBean.credentialsConfigBean.resolveUsername(
            getContext(),
            "CREDENTIALS",
            "conf.credentialsConfigBean.",
            issues
        );
        String password = databricksConfigBean.credentialsConfigBean.resolvePassword(
            getContext(),
            "CREDENTIALS",
            "conf.credentialsConfigBean.",
            issues
        );
        auth = HttpAuthenticationFeature.basic(username, password);
        builder.register(auth);
      } else {
        String token = databricksConfigBean.credentialsConfigBean.resolveToken(
            getContext(),
            "CREDENTIALS",
            "conf.credentialsConfigBean.",
            issues
        );

        builder.register((ClientRequestFilter) requestContext ->
            requestContext.getHeaders().add("Authorization", "Bearer " + token)
        );
      }

      JerseyClientUtil.configureSslContext(databricksConfigBean.tlsConfigBean, builder);

      if(databricksConfigBean.useProxy) {
        JerseyClientUtil.configureProxy(
            proxyConf.uri,
            proxyUsername,
            proxyPassword,
            builder
        );
      }

      client = builder.build();
      validateWithDatabricks(getContext(), issues);
    }
    return issues;
  }

  @VisibleForTesting
  protected ClientBuilder getClientBuilder() {
    return ClientBuilder.newBuilder();
  }

  private void validateWithDatabricks(Stage.Context context, List<Stage.ConfigIssue> issues) {

    try {
      Response listResponse = client.target(baseUrl + LIST_URL)
          .queryParam("job_id", databricksConfigBean.jobId)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .buildGet()
          .invoke();

      switch (listResponse.getStatus()) {
        case 400:
          issues.add(context.createConfigIssue(
              APPLICATION_GROUP, PREFIX + "jobId", DATABRICKS_01, databricksConfigBean.jobId));
          break;
        case 401:
        case 403:
          String credentialField =
              databricksConfigBean.credentialsConfigBean.credentialType == CredentialType.PASSWORD ?
                  "conf.credentialsConfigBean.username" :
                  "conf.credentialsConfigBean.token";
          issues.add(context.createConfigIssue(
              "CREDENTIALS",
              credentialField,
              DATABRICKS_05,
              databricksConfigBean.jobId
          ));
          break;
        case 200:
          verifyTaskType(context, issues, listResponse);
          break;
        default:
          issues.add(context.createConfigIssue(
              APPLICATION_GROUP, PREFIX + "jobId", DATABRICKS_03));
      }

    } catch (Exception ex) {
      LOG.error("Error while listing job id", ex);
      issues.add(context.createConfigIssue(SPARK_GROUP, PREFIX + "baseUrl", DATABRICKS_02));
    }
  }

  @SuppressWarnings("unchecked")
  private void verifyTaskType(Stage.Context context, List<Stage.ConfigIssue> issues, Response listResponse) {
    listResponse.bufferEntity();

    Map<Object, Object> result = listResponse.readEntity(Map.class);
    Map<String, Object> settings = (Map<String, Object>)result.get("settings");
    if ((databricksConfigBean.jobType == JAR && !verifyJarTask(settings))
        || (databricksConfigBean.jobType == NOTEBOOK && !verifyNotebookTask(settings))) {
      JobType expected = databricksConfigBean.jobType;
      // We know we didn't get the expected type back, so the actual one must be the other type
      JobType actual = expected == JAR ? NOTEBOOK : JAR;
      issues.add(context.createConfigIssue(
          APPLICATION_GROUP, PREFIX + "jobType", DATABRICKS_04,
          databricksConfigBean.jobId, expected.getLabel(), actual.getLabel()));
    }
  }

  @SuppressWarnings("unchecked")
  private boolean verifyJarTask(Map<String, Object> settings) {
    return settings.containsKey("spark_jar_task");
  }

  @SuppressWarnings("unchecked")
  private boolean verifyNotebookTask(Map<String, Object> settings) {
    return settings.containsKey("notebook_task");
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      Entity e;
      if (databricksConfigBean.jobType == JAR) {
        List<String> params = databricksConfigBean.evaluateJarELs(record);
        e = Entity.json(new RunJarJobJson(databricksConfigBean.jobId, params));
      } else {
        Map<String, String> params = databricksConfigBean.evaluateNotebookELs(record);
        e = Entity.json(new RunNotebookJobJson(databricksConfigBean.jobId, params));
      }

      Response response = client.target(baseUrl + RUN_NOW_URL)
          .request()
          .buildPost(e)
          .invoke();

      if (response.getStatus() != 200) {
        throw new StageException(DATABRICKS_06, databricksConfigBean.jobId, response.readEntity(String.class));
      } else {
        RunJobResponseJson responseJson = response.readEntity(RunJobResponseJson.class);
        LOG.info("Job launched. The sequence number of this run among all runs of the job is: "
            + responseJson.getNumberInJob());
        final int runId = responseJson.getRunId();
        LOG.info(Utils.format("Spark application launched with app id: '{}'", runId));
        JOB_CREATED.create(getContext()).with(APP_ID, runId).createAndSend();
      }
    }
  }
}
