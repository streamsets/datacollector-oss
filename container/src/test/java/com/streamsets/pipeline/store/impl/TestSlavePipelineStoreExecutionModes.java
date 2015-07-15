/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dc.execution.store.SlavePipelineStateStore;
import com.streamsets.dc.execution.store.TestPipelineStateStore;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;


public class TestSlavePipelineStoreExecutionModes {

  @Test
  public void testSlaveExecutionModeStoreDir() {
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo =
      new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestPipelineStateStore.class.getClassLoader()));
    StageLibraryTask stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    runtimeInfo.setExecutionMode(RuntimeInfo.ExecutionMode.SLAVE.name());
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask,
      new SlavePipelineStateStore());
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
    runtimeInfo.setExecutionMode(RuntimeInfo.ExecutionMode.STANDALONE.name());
    FilePipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask,
      new SlavePipelineStateStore());
    SlavePipelineStoreTask slavePipelineStoreTask = new SlavePipelineStoreTask(pipelineStoreTask);
    slavePipelineStoreTask.init();
    assertEquals(new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR).getAbsolutePath(),
      pipelineStoreTask.getStoreDir().getAbsolutePath());
    pipelineStoreTask.getStoreDir().delete();
  }
}
