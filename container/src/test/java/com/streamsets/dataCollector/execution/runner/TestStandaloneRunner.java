/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.PipelineManager;
import com.streamsets.dataCollector.execution.store.FilePipelineStateStore;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.TestUtil;

public class TestStandaloneRunner {

  private Manager pipelineManager;
  private SafeScheduledExecutorService previewExecutorService;
  private SafeScheduledExecutorService runnerExecutorService;
  private RuntimeInfo runtimeInfo;
  private PipelineStateStore pipelineStateStore;
  private PipelineStoreTask pipelineStore;
  private com.streamsets.pipeline.util.Configuration configuration;
  private StageLibraryTask stageLibrary;

  @BeforeClass
  public static void beforeClass() throws IOException {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
    TestUtil.captureStagesForProductionRun();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  private void setupExecutors() {
    previewExecutorService = new SafeScheduledExecutorService(10, "PreviewExecutor");
    runnerExecutorService = new SafeScheduledExecutorService(10, "RunnerExecutor");
  }

  @Before
  public void setUp() throws IOException {
    TestUtil.EMPTY_OFFSET = false;
    stageLibrary = new TestUtil.TestStageLibraryModule().provideStageLibrary();
    runtimeInfo  = new TestUtil.TestRuntimeModule().provideRuntimeInfo();
    configuration = new com.streamsets.pipeline.util.Configuration();
    pipelineStateStore = new  FilePipelineStateStore(runtimeInfo, configuration);
    pipelineStore = new TestUtil.TestPipelineStoreModuleNew().providePipelineStore(runtimeInfo, stageLibrary, pipelineStateStore);
    setupExecutors();
    pipelineManager = new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
      previewExecutorService, runnerExecutorService);

    pipelineManager.init();
  }

  @After
  public void tearDown() throws Exception {
    LogUtil.unregisterAllLoggers();
    try {
    pipelineManager.stop();
    } catch(Exception e) {
      //
    }
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);

  }

  @Test(timeout=20000)
  public void testPipelineStart() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while(runner.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner.stop();
    while(runner.getStatus()!=PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout=20000)
  public void testPipelineFinish() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while(runner.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while(runner.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }

  }

  @Test(timeout=20000)
  public void testDisconnectedPipelineStartedAgain() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while(runner.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    //sdc going down
    pipelineManager.stop();
    while(runner.getStatus()!=PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }

    setupExecutors();
    pipelineManager = new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
      previewExecutorService, runnerExecutorService);

    pipelineManager.init();
    while(pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }

  }

  @Test(timeout=20000)
  public void testFinishedPipelineNotStartingAgain() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while(runner.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while(runner.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }

    //sdc going down
    pipelineManager.stop();
    // Simulate finishing, the runner shouldn't restart on finishing
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHING, null, null, ExecutionMode.STANDALONE);
    setupExecutors();
    pipelineManager = new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
      previewExecutorService, runnerExecutorService);

    pipelineManager.init();
    while(runner.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout=20000)
  public void testMultiplePipelineStartStop() throws Exception {
    Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");

    runner1.start();
    runner2.start();
    while(runner1.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while(runner2.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner1.stop();
    while(runner1.getStatus()!=PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }
    assertEquals(PipelineStatus.RUNNING, runner2.getStatus());
    runner2.stop();
    while(runner2.getStatus()!=PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }

  }

  @Test(timeout=20000)
  public void testMultiplePipelineFinish() throws Exception {
    Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");

    runner1.start();
    runner2.start();
    while(runner1.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while(runner2.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while(runner1.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
    while(runner2.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout=20000)
  public void testDisconnectedPipelinesStartedAgain() throws Exception {
    Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");
    runner1.start();
    runner2.start();
    while(runner1.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while(runner2.getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    //sdc going down
    pipelineManager.stop();
    while(runner1.getStatus()!=PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }
    while(runner2.getStatus()!=PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }
    setupExecutors();
    pipelineManager = new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
      previewExecutorService, runnerExecutorService);

    pipelineManager.init();
    while(pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while(pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
  }

}
