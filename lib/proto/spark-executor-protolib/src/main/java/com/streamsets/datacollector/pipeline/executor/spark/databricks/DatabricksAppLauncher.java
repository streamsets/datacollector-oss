/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.pipeline.executor.spark.databricks;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.pipeline.executor.spark.AppLauncher;
import com.streamsets.datacollector.pipeline.executor.spark.ApplicationLaunchFailureException;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutorConfigBean;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.http.GrizzlyClientCustomizer;
import com.streamsets.pipeline.lib.http.JerseyClientUtil;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_11;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_12;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_13;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_14;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_15;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_16;
import static com.streamsets.datacollector.pipeline.executor.spark.databricks.JobType.JAR;
import static com.streamsets.datacollector.pipeline.executor.spark.databricks.JobType.NOTEBOOK;

public class DatabricksAppLauncher implements AppLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DatabricksAppLauncher.class);
  private DatabricksConfigBean databricksConfigBean;
  private Client client;
  private static final String SPARK_GROUP = "SPARK";
  private static final String APPLICATION_GROUP = "APPLICATION";
  private static final String PREFIX = "conf.databricksConfigBean.";

  private static final String RUN_NOW_URL = "/api/2.0/jobs/run-now";
  private static final String LIST_URL = "/api/2.0/jobs/get";

  private String baseUrl;

  @Override
  public List<Stage.ConfigIssue> init(Stage.Context context, SparkExecutorConfigBean configs) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    databricksConfigBean = configs.databricksConfigBean;
    Optional
        .ofNullable(databricksConfigBean.init(context, PREFIX))
        .ifPresent(issues::addAll);

    baseUrl = databricksConfigBean.baseUrl.endsWith("/") ?
        databricksConfigBean.baseUrl.substring(0, databricksConfigBean.baseUrl.length() - 1) :
        databricksConfigBean.baseUrl;

    com.streamsets.pipeline.lib.http.HttpProxyConfigBean proxyConf = databricksConfigBean.proxyConfigBean
        .getUnderlyingConfig();

    boolean useProxy = !StringUtils.isEmpty(proxyConf.uri);
    String proxyUsername = null;
    String proxyPassword = null;
    if(useProxy) {
      proxyUsername = proxyConf.resolveUsername(context, "PROXY", "conf.databricksConfigBean.proxyConfigBean.", issues);
      proxyPassword = proxyConf.resolvePassword(context, "PROXY", "conf.databricksConfigBean.proxyConfigBean.", issues);
    }

    if(issues.isEmpty()) {
      ClientConfig clientConfig = new ClientConfig()
        .property(ClientProperties.ASYNC_THREADPOOL_SIZE, 1)
        .property(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);

      if(useProxy) {
       clientConfig = clientConfig.connectorProvider(new GrizzlyConnectorProvider(new GrizzlyClientCustomizer(
           useProxy,
           proxyUsername,
           proxyPassword
        )));
      }

      HttpAuthenticationFeature auth = configs.credentialsConfigBean.init();
      ClientBuilder builder = getClientBuilder()
        .withConfig(clientConfig)
        .register(JacksonJsonProvider.class);

      if (auth != null) {
        builder.register(auth);
      }

      JerseyClientUtil.configureSslContext(databricksConfigBean.sslConfigBean.getUnderlyingConfig(), builder);

      if(useProxy) {
        JerseyClientUtil.configureProxy(
          proxyConf.uri,
          proxyUsername,
          proxyPassword,
          builder
        );
      }

      client = builder.build();
      validateWithDatabricks(context, issues);
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
                  APPLICATION_GROUP, PREFIX + "jobId", SPARK_EXEC_11, databricksConfigBean.jobId));
          break;
        case 401:
        case 403:
          issues.add(context.createConfigIssue(
                  "CREDENTIALS", "conf.credentialsConfigBean.username",
                  SPARK_EXEC_15, databricksConfigBean.jobId));
          break;
        case 200:
          verifyTaskType(context, issues, listResponse);
          break;
        default:
          issues.add(context.createConfigIssue(
                  APPLICATION_GROUP, PREFIX + "jobId", SPARK_EXEC_13));
      }

    } catch (Exception ex) {
      LOG.error("Error while listing job id", ex);
      issues.add(context.createConfigIssue(SPARK_GROUP, PREFIX + "baseUrl", SPARK_EXEC_12));
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
          APPLICATION_GROUP, PREFIX + "jobType", SPARK_EXEC_14,
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
  public Optional<String> launchApp(Record record) throws
      ApplicationLaunchFailureException, ELEvalException {
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
      throw new ApplicationLaunchFailureException(
          new StageException(SPARK_EXEC_16, databricksConfigBean.jobId, response.readEntity(String.class)));
    } else {
      RunJobResponseJson responseJson = response.readEntity(RunJobResponseJson.class);
      LOG.info("Job launched. The sequence number of this run among all runs of the job is: "
          + responseJson.getNumberInJob());
      return Optional.of(String.valueOf(responseJson.getRunId()));
    }
  }

  @Override
  public boolean waitForCompletion() throws InterruptedException {
    return false;
  }

  @Override
  public void close() {
    client.close();
  }
}
