/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
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
import com.streamsets.dataCollector.execution.store.CachePipelineStateStore;
import com.streamsets.dataCollector.execution.store.FilePipelineStateStore;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.runner.StagePipe;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
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


  @Before
  public void setUp() throws IOException {
    System.setProperty(StagePipe.SKIP_RUNTIME_STATS_METRIC, "true");
    stageLibrary = new TestUtil.TestStageLibraryModule().provideStageLibrary();
    runtimeInfo = new TestUtil.TestRuntimeModule().provideRuntimeInfo();
    configuration = new com.streamsets.pipeline.util.Configuration();
    pipelineStateStore = new CachePipelineStateStore(new FilePipelineStateStore(runtimeInfo, configuration));
    pipelineStore =
      new TestUtil.TestPipelineStoreModuleNew().providePipelineStore(runtimeInfo, stageLibrary, pipelineStateStore);
    previewExecutorService = new SafeScheduledExecutorService(1, "PreviewExecutor");
    runnerExecutorService = new SafeScheduledExecutorService(10, "RunnerExecutor");
    pipelineManager =
      new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
        previewExecutorService, runnerExecutorService, runnerExecutorService);

    pipelineManager.init();
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.EMPTY_OFFSET = false;
    System.setProperty(StagePipe.SKIP_RUNTIME_STATS_METRIC, "false");
    try {
      pipelineManager.stop();
    } catch (Exception e) {
      //
    }
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    try {
      FileUtils.deleteDirectory(f);
    } catch (Exception e) {

    }
    previewExecutorService.shutdownNow();
    runnerExecutorService.shutdownNow();
  }

  @Test(timeout = 20000)
  public void testPipelineStart() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");

    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner.stop();
    while (runner.getStatus() != PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 5000)
  public void testPipelinePrepare() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.FINISHED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.STOPPED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.DISCONNECTING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.CONNECTING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STARTING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, runner.getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.DISCONNECTED, null, null,
      ExecutionMode.STANDALONE);
    runner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, runner.getStatus());
  }

  @Test(timeout = 20000)
  public void testPipelineFinish() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while (runner.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }

  }

  @Test(timeout = 20000)
  public void testDisconnectedPipelineStartedAgain() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    // sdc going down
    pipelineManager.stop();
    while (runner.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }

    pipelineManager =
      new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
        previewExecutorService, runnerExecutorService, runnerExecutorService);

    pipelineManager.init();
    while (pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin").getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }

  }

  @Test(timeout = 20000)
  public void testFinishedPipelineNotStartingAgain() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while (runner.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }

    // sdc going down
    pipelineManager.stop();
    // Simulate finishing, the runner shouldn't restart on finishing
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHING, null, null,
      ExecutionMode.STANDALONE);
    pipelineManager =
      new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
        previewExecutorService, runnerExecutorService, runnerExecutorService);

    pipelineManager.init();
    while (runner.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void testMultiplePipelineStartStop() throws Exception {

    try {
      Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
      Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");
      runner1.start();
      runner2.start();
      while (runner1.getStatus() != PipelineStatus.RUNNING) {
        Thread.sleep(100);
      }
      while (runner2.getStatus() != PipelineStatus.RUNNING) {
        Thread.sleep(100);
      }
      runner1.stop();
      while (runner1.getStatus() != PipelineStatus.STOPPED) {
        Thread.sleep(100);
      }
      assertEquals(PipelineStatus.RUNNING, runner2.getStatus());
      runner2.stop();
      while (runner2.getStatus() != PipelineStatus.STOPPED) {
        Thread.sleep(100);
      }
    } finally {
      System.setProperty(StagePipe.SKIP_RUNTIME_STATS_METRIC, "false");
    }

  }

  @Test(timeout = 20000)
  public void testMultiplePipelineFinish() throws Exception {
    Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");

    runner1.start();
    runner2.start();
    while (runner1.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while (runner2.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while (runner1.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
    while (runner2.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void testDisconnectedPipelinesStartedAgain() throws Exception {
    Runner runner1 = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin");
    Runner runner2 = pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2");
    runner1.start();
    runner2.start();
    while (runner1.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while (runner2.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    // sdc going down
    pipelineManager.stop();
    while (runner1.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }
    while (runner2.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }

    pipelineManager =
      new PipelineManager(runtimeInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary,
        previewExecutorService, runnerExecutorService, runnerExecutorService);

    pipelineManager.init();
    while (pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0", "admin").getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while (pipelineManager.getRunner(TestUtil.MY_SECOND_PIPELINE, "0", "admin2").getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
  }

}
