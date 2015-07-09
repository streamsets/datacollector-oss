/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.util.TestUtil;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestStandaloneRunner {

  private static Logger LOG = LoggerFactory.getLogger(TestStandaloneRunner.class);

  private Manager pipelineManager;
  private PipelineStateStore pipelineStateStore;

  @BeforeClass
  public static void beforeClass() throws IOException {

  }

  @AfterClass
  public static void afterClass() throws IOException {

  }

  @Before
  public void setUp() throws IOException {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
    TestUtil.captureStagesForProductionRun();
    TestUtil.EMPTY_OFFSET = false;
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.EMPTY_OFFSET = false;
    pipelineManager.stop();
    try {
      File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
      FileUtils.deleteDirectory(f);
    } catch (Exception e) {

    }
    TestUtil.EMPTY_OFFSET = false;
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test(timeout = 20000)
  public void testPipelineStart() throws Exception {
    Runner runner = pipelineManager.getRunner("admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner.stop();
    while (runner.getStatus() != PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 50000)
  public void testPipelinePrepare() throws Exception {
    Runner runner = pipelineManager.getRunner("admin", TestUtil.MY_PIPELINE, "0");
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
    Runner runner = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while (runner.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 2000000000)
  public void testDisconnectedPipelineStartedAgain() throws Exception {
    Runner runner = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    // sdc going down
    pipelineManager.stop();
    while (runner.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }

    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();

    while(pipelineManager.getRunner("admin", TestUtil.MY_PIPELINE, "0").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void testFinishedPipelineNotStartingAgain() throws Exception {
    Runner runner = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
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
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();

    //Since SDC went down we need to get the runner again
    runner = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    while(runner.getStatus()!=PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 20000)
  public void testMultiplePipelineStartStop() throws Exception {
    Runner runner1 = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner("admin2", TestUtil.MY_SECOND_PIPELINE, "0");

    runner1.start();
    runner2.start();
    while (runner1.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(1000);
    }
    while (runner2.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(1000);
    }
    runner1.stop();
    while (runner1.getStatus() != PipelineStatus.STOPPED) {
      Thread.sleep(1000);
    }
    assertEquals(PipelineStatus.RUNNING, runner2.getStatus());
    runner2.stop();
    while (runner2.getStatus() != PipelineStatus.STOPPED) {
      Thread.sleep(1000);
    }
  }

  @Test(timeout = 20000)
  public void testMultiplePipelineFinish() throws Exception {
    Runner runner1 = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner("admin2", TestUtil.MY_SECOND_PIPELINE, "0");

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
    Runner runner1 = pipelineManager.getRunner( "admin", TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner("admin2", TestUtil.MY_SECOND_PIPELINE, "0");
    runner1.start();
    runner2.start();
    while (runner1.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(1000);
    }
    while (runner2.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(1000);
    }
    // sdc going down
    pipelineManager.stop();
    while (runner1.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(1000);
    }
    while (runner2.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(1000);
    }

    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    Thread.sleep(2000);

    while(pipelineManager.getRunner("admin", TestUtil.MY_PIPELINE, "0").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    while(pipelineManager.getRunner("admin2", TestUtil.MY_SECOND_PIPELINE, "0").getStatus()!=PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
  }

}
