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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class TestDatabricksJobExecutor {

  private ClientBuilder clientBuilder;
  private DatabricksJobExecutor executor;
  private DatabricksConfigBean configBean;
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
    doReturn(listTarget).when(listTarget).queryParam("job_id", configBean.jobId);
    doReturn(builder).when(listTarget).request(MediaType.APPLICATION_JSON_TYPE);
    doReturn(getInvocation).when(builder).buildGet();
    doReturn(listResponse).when(getInvocation).invoke();
    doReturn(postInvocation).when(builder).buildPost(any());
    doReturn(runResponse).when(postInvocation).invoke();

    doReturn(builder).when(listTarget).request();

    doReturn(postInvocation).when(builder)
        .buildPost(Entity.json((
            new RunJarJobJson(configBean.jobId, configBean.jarParams))));

    doReturn(client).when(clientBuilder).build();

  }

  private void configureBean() {
    configBean = new DatabricksConfigBean();
    configBean.baseUrl = "https://testurl.com:1009";
    configBean.jobId = 1000;
    configBean.jarParams = ImmutableList.of("param1", "param2");
    configBean.jobType = JobType.JAR;
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
    executor = new FakeDatabricksJobExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(DatabricksJobLauncherDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    runner.runWrite(ImmutableList.of(RecordCreator.create()));
    runner.runDestroy();
  }

  @Test
  public void testRunJarJobConfiguredNotebook() throws Exception {
    configBean.jobType = JobType.NOTEBOOK;
    configBean.notebookParams = ImmutableMap.of("key1", "param2");
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("spark_jar_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();
    doTestFail(200, Errors.DATABRICKS_04);
  }

  @Test
  public void testRunNotebookJobConfiguredJar() throws Exception {
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("notebook_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();
    doTestFail(200, Errors.DATABRICKS_04);
  }

  @Test
  public void testRunAuthFail() throws Exception {
    doTestFail(401, Errors.DATABRICKS_05);
    doTestFail(403, Errors.DATABRICKS_05);
  }

  @Test
  public void testListFail() throws Exception {
    doTestFail(400, Errors.DATABRICKS_01);
    doTestFail(500, Errors.DATABRICKS_03);
  }

  @Test
  public void testBaseURLError() throws Exception {
    doThrow(new RuntimeException()).when(getInvocation).invoke();
    executor = new FakeDatabricksJobExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(DatabricksJobLauncherDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(Errors.DATABRICKS_02.getCode()));
    }
  }

  @Test
  public void testRunError() throws Exception {
    Map<Object, Object> mapWithJob =
        ImmutableMap.of("settings", ImmutableMap.of("spark_jar_task", 1000));
    doReturn(mapWithJob).when(listResponse).readEntity(Map.class);
    doReturn(200).when(listResponse).getStatus();

    doReturn(500).when(runResponse).getStatus();
    executor = new FakeDatabricksJobExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(DatabricksJobLauncherDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      runner.runWrite(ImmutableList.of(RecordCreator.create()));
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertEquals(Errors.DATABRICKS_06, ex.getErrorCode());
    }
    runner.runDestroy();
  }

  private void doTestFail(int statusToReturn, Errors expectedError) {
    doReturn(statusToReturn).when(listResponse).getStatus();
    executor = new FakeDatabricksJobExecutor(configBean);
    ExecutorRunner runner = new ExecutorRunner.Builder(DatabricksJobLauncherDExecutor.class, executor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    try {
      runner.runInit();
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertTrue(ex.getMessage().contains(expectedError.getCode()));
    }
  }

  private class FakeDatabricksJobExecutor extends DatabricksJobExecutor {

    @Override
    public ClientBuilder getClientBuilder() {
      return clientBuilder;
    }

    FakeDatabricksJobExecutor(DatabricksConfigBean configs) {
      super(configs);
    }

  }

}
