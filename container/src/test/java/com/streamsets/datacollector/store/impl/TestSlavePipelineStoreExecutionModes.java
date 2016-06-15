/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStore;
import com.streamsets.datacollector.execution.store.TestPipelineStateStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;


public class TestSlavePipelineStoreExecutionModes {

  @Test
  public void testSlaveExecutionModeStoreDir() {
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo =
      new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestPipelineStateStore.class.getClassLoader()));
    StageLibraryTask stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask,
      new SlavePipelineStateStore(), new LockCache<String>());
    SlavePipelineStoreTask slavePipelineStoreTask = new SlavePipelineStoreTask(pipelineStoreTask);
    slavePipelineStoreTask.init();
    assertEquals(new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR).getAbsolutePath(), pipelineStoreTask.getStoreDir().getAbsolutePath());
    pipelineStoreTask.getStoreDir().delete();
  }

  @Test
  public void testStandaloneExecutionModeStoreDir() {
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo =
      new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestPipelineStateStore.class.getClassLoader()));
    StageLibraryTask stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask,
      new SlavePipelineStateStore(), new LockCache<String>());
    SlavePipelineStoreTask slavePipelineStoreTask = new SlavePipelineStoreTask(pipelineStoreTask);
    slavePipelineStoreTask.init();
    assertEquals(new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR).getAbsolutePath(),
      pipelineStoreTask.getStoreDir().getAbsolutePath());
    pipelineStoreTask.getStoreDir().delete();
  }
}
