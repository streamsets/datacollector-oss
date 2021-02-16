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
package com.streamsets.datacollector.execution.dagger;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.main.MainSlavePipelineManagerModule;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.PipelineTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.SlavePipelineTask;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestPipelineManagerModule {

  @Before
  public void setup() throws IOException {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  private static void setDummyDpmClientInfo(RuntimeInfo runtimeInfo) {
    runtimeInfo.setAttribute(
        DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY,
        new DpmClientInfo() {
          @Override
          public String getDpmBaseUrl() {
            return "http://localhost:18631";
          }

          @Override
          public Map<String, String> getHeaders() {
            return ImmutableMap.of(
                SSOConstants.X_APP_COMPONENT_ID, "componentId",
                SSOConstants.X_APP_AUTH_TOKEN, "authToken"
            );
          }

          @Override
          public void setDpmBaseUrl(String dpmBaseUrl) {

          }
        });
  }

  @Test
  public void testStandalonePipelineManagerModule() throws PipelineException {
    //Start SDC and get an instance of PipelineTask
    ObjectGraph objectGraph = ObjectGraph.create(MainStandalonePipelineManagerModule.createForTest(AsterModuleForTest.class));
    setDummyDpmClientInfo(objectGraph.get(RuntimeInfo.class));
    TaskWrapper taskWrapper = objectGraph.get(TaskWrapper.class);
    Assert.assertTrue(taskWrapper.getTask() instanceof PipelineTask);
    Assert.assertNotNull(objectGraph.get(Configuration.class));

    //Get an instance of manager
    taskWrapper.init();
    taskWrapper.run();
    PipelineTask pipelineTask = (PipelineTask) taskWrapper.getTask();
    Manager pipelineManager = pipelineTask.getManager();
    Assert.assertTrue(pipelineManager instanceof StandaloneAndClusterPipelineManager);

    PipelineStoreTask pipelineStoreTask = pipelineTask.getPipelineStoreTask();
    PipelineConfiguration pc = pipelineStoreTask.create("user", "p1", "p1", "description", false, false,
        new HashMap<String, Object>()
    );
    //Create previewer
    Previewer previewer = pipelineManager.createPreviewer(
        "user",
        pc.getInfo().getPipelineId(),
        "1",
        Collections.emptyList(),
        p -> null,
        false,
        new HashMap<>()
    );
    assertEquals(previewer, pipelineManager.getPreviewer(previewer.getId()));
    ((StandaloneAndClusterPipelineManager)pipelineManager).outputRetrieved(previewer.getId());
    assertNull(pipelineManager.getPreviewer(previewer.getId()));

    pipelineStoreTask.save("user", pc.getInfo().getPipelineId(), "0", "description", pc, false);

    //create Runner
    Runner runner = pipelineManager.getRunner(pc.getInfo().getPipelineId(), "0");
    Assert.assertNotNull(runner.getRunner(AsyncRunner.class));

    runner = runner.getRunner(StandaloneRunner.class);
    Assert.assertNotNull(runner);

    Assert.assertEquals(PipelineStatus.EDITED, runner.getState().getStatus());
    Assert.assertEquals(pc.getInfo().getPipelineId(), runner.getName());
    Assert.assertEquals("0", runner.getRev());

    taskWrapper.stop();
  }

  @Test
  public void testSlavePipelineManagerModule() throws PipelineException {
    ObjectGraph objectGraph = ObjectGraph.create(MainSlavePipelineManagerModule.createForTest(AsterModuleForTest.class));
    ((SlaveRuntimeInfo)objectGraph.get(RuntimeInfo.class)).setId("id");
    setDummyDpmClientInfo(objectGraph.get(RuntimeInfo.class));
    TaskWrapper taskWrapper = objectGraph.get(TaskWrapper.class);
    taskWrapper.init();
    Assert.assertTrue(taskWrapper.getTask() instanceof SlavePipelineTask);
    PipelineTask pipelineTask = (PipelineTask) taskWrapper.getTask();
    Manager pipelineManager = pipelineTask.getManager();
    Assert.assertTrue(pipelineManager instanceof SlavePipelineManager);

    try {
      pipelineManager.createPreviewer("user", "p1", "1", Collections.emptyList(), p -> null, false, new HashMap<>());
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    PipelineStoreTask pipelineStoreTask = pipelineTask.getPipelineStoreTask();
    Assert.assertTrue(pipelineStoreTask instanceof SlavePipelineStoreTask);

    try {
      pipelineStoreTask.create("user", "p1", "p1", "description", false, false, new HashMap<String, Object>());
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    try {
      pipelineStoreTask.save("user", "p1", "0", "description", Mockito.mock(PipelineConfiguration.class), false);
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    Runner runner = pipelineManager.getRunner("p1", "0");
    Assert.assertTrue(runner instanceof AsyncRunner);

    AsyncRunner asyncRunner = (AsyncRunner)runner;

    Assert.assertEquals(PipelineStatus.EDITED, asyncRunner.getState().getStatus());
    Assert.assertEquals("p1", asyncRunner.getName());
    Assert.assertEquals("0", asyncRunner.getRev());

    taskWrapper.stop();
  }
}
