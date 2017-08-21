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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.pipeline.executor.spark.AppLauncher;
import com.streamsets.datacollector.pipeline.executor.spark.ApplicationLaunchFailureException;
import com.streamsets.datacollector.pipeline.executor.spark.Errors;
import com.streamsets.datacollector.pipeline.executor.spark.SparkDExecutor;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutor;
import com.streamsets.datacollector.pipeline.executor.spark.SparkExecutorConfigBean;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ExecutorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyInvocation;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_11;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_12;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_13;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_14;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_15;
import static com.streamsets.datacollector.pipeline.executor.spark.Errors.SPARK_EXEC_16;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TestSparkExecutor {

  private ClientBuilder clientBuilder;
  private SparkExecutor executor;
  private SparkExecutorConfigBean configBean;
  private JerseyInvocation getInvocation;
  private Response listResponse;
  private Response runResponse;

  @Before
  public void setUp() {
    configureBean();
    clientBuilder = spy(ClientBuilder.newBuilder());
    JerseyClient client = mock(JerseyClient.class);
    JerseyWebTarget listTarget = mock(JerseyWebTarget.class);
    JerseyInvocation.Builder builder = mock(JerseyInvocation.Builder.class);
    getInvocation = mock(JerseyInvocation.class);
    JerseyInvocation postInvocation = mock(JerseyInvocation.class);
    listResponse = mock(Response.class);
    runResponse = mock(Response.class);

    doReturn(listTarget).when(client).target(anyString());
    doReturn(listTarget).when(listTarget).queryParam("job_id", configBean.databricksConfigBean.jobId);
    doReturn(builder).when(listTarget).request(MediaType.APPLICATION_JSON_TYPE);
    doReturn(getInvocation).when(builder).buildGet();
    doReturn(listResponse).when(getInvocation).invoke();
    doReturn(postInvocation).when(builder).buildPost(any());
    doReturn(runResponse).when(postInvocation).invoke();

    doReturn(builder).when(listTarget).request();

    doReturn(postInvocation).when(builder)
        .buildPost(Entity.json((
            new RunJarJobJson(configBean.databricksConfigBean.jobId, configBean.databricksConfigBean.jarParams))));

    doReturn(client).when(clientBuilder).build();

  }

  private void configureBean() {
    configBean = new SparkExecutorConfigBean();
    configBean.databricksConfigBean = new DatabricksConfigBean();
    configBean.databricksConfigBean.baseUrl = "https://testurl.com:1009";
    configBean.databricksConfigBean.jobId = 1000;
    configBean.databricksConfigBean.jarParams = ImmutableList.of("param1", "param2");
    configBean.databricksConfigBean.jobType = JobType.JAR;
  }

  @Test
  public void testRunSuccess() throws Exception {
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("spark_jar_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();

    doReturn(200).when(runResponse).getStatus();
    doReturn(new RunJobResponseJson(1356, 9945))
        .when(runResponse).readEntity(RunJobResponseJson.class);
    executor = new ExtendedSparkExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runWrite(ImmutableList.of(RecordCreator.create()));
    runner.runDestroy();
  }

  @Test
  public void testRunJarJobConfiguredNotebook() throws Exception {
    configBean.databricksConfigBean.jobType = JobType.NOTEBOOK;
    configBean.databricksConfigBean.notebookParams = ImmutableMap.of("key1", "param2");
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("spark_jar_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();
    doTestFail(200, SPARK_EXEC_14);
  }

  @Test
  public void testRunNotebookJobConfiguredJar() throws Exception {
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("notebook_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();
    doTestFail(200, SPARK_EXEC_14);
  }

  @Test
  public void testRunAuthFail() throws Exception {
    doTestFail(401, SPARK_EXEC_15);
    doTestFail(403, SPARK_EXEC_15);
  }

  @Test
  public void testListFail() throws Exception {
    doTestFail(400, SPARK_EXEC_11);
    doTestFail(500, SPARK_EXEC_13);
  }

  @Test
  public void testBaseURLError() throws Exception {
    doThrow(new RuntimeException()).when(getInvocation).invoke();
    executor = new ExtendedSparkExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(SPARK_EXEC_12.getCode()));
    }
  }

  @Test
  public void testRunError() throws Exception {
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("spark_jar_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();

    doReturn(500).when(runResponse).getStatus();
    executor = new ExtendedSparkExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      runner.runWrite(ImmutableList.of(RecordCreator.create()));
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getCause() instanceof ApplicationLaunchFailureException);
      ApplicationLaunchFailureException launchFailureException = (ApplicationLaunchFailureException) ex.getCause();
      Assert.assertTrue(launchFailureException.getCause() instanceof StageException);
      Assert.assertEquals(SPARK_EXEC_16, ((StageException)launchFailureException.getCause()).getErrorCode());
    }
    runner.runDestroy();
  }

  private void doTestFail(int statusToReturn, Errors expectedError) {
    doReturn(statusToReturn).when(listResponse).getStatus();
    executor = new ExtendedSparkExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(SparkDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(expectedError.getCode()));
    }
  }

  private class FakeDatabricksLauncher extends DatabricksAppLauncher {

    @Override
    public ClientBuilder getClientBuilder() {
      return clientBuilder;
    }
  }

  protected class ExtendedSparkExecutor extends SparkExecutor {

    ExtendedSparkExecutor(SparkExecutorConfigBean configs) {
      super(configs);
    }

    @Override
    public AppLauncher getLauncher() {
      return new FakeDatabricksLauncher();
    }
  }

}
