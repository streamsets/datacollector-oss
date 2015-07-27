/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store.impl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

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
      new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
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
      new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
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
