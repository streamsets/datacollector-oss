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
package com.streamsets.datacollector.store.impl;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStore;
import com.streamsets.datacollector.execution.store.TestPipelineStateStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import com.streamsets.pipeline.BootstrapMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class TestSlavePipelineStoreExecutionModes {
  private final BuildInfo buildInfo = ProductBuildInfo.getDefault();

  @Before
  public void setup() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
  }

  @After
  public void tearDown() {
    System.clearProperty("sdc.testing-mode");
  }

  @Test
  public void testSlaveExecutionModeStoreDir() throws Exception {
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo =
      new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestPipelineStateStore.class.getClassLoader()));
    StageLibraryTask stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(
        buildInfo,
        runtimeInfo,
        stageLibraryTask,
        new SlavePipelineStateStore(),
        new EventListenerManager(),
        new LockCache<>(),
        Mockito.mock(PipelineCredentialHandler.class),
        new Configuration()
    );
    SlavePipelineStoreTask slavePipelineStoreTask = new SlavePipelineStoreTask(pipelineStoreTask);
    slavePipelineStoreTask.init();
    assertEquals(Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR), pipelineStoreTask.getStoreDir());
    Files.delete(pipelineStoreTask.getStoreDir());
  }

  @Test
  public void testStandaloneExecutionModeStoreDir() throws Exception {
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo =
      new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestPipelineStateStore.class.getClassLoader()));
    StageLibraryTask stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(
        buildInfo,
        runtimeInfo,
        stageLibraryTask,
        new SlavePipelineStateStore(),
        new EventListenerManager(),
        new LockCache<>(),
        Mockito.mock(PipelineCredentialHandler.class),
        new Configuration()
    );
    SlavePipelineStoreTask slavePipelineStoreTask = new SlavePipelineStoreTask(pipelineStoreTask);
    slavePipelineStoreTask.init();
    assertEquals(Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR), pipelineStoreTask.getStoreDir());
    Files.delete(pipelineStoreTask.getStoreDir());
  }
}
