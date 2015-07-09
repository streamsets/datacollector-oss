/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.SlaveManager;
import com.streamsets.dataCollector.execution.manager.TestSlaveManager;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.util.TestUtil;
import com.streamsets.pipeline.util.TestUtil.TestPipelineProviderModule;

import dagger.ObjectGraph;

public class TestSlaveStandaloneRunner {

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

  @After
  public void tearDown() throws Exception {
    TestUtil.EMPTY_OFFSET = false;
    File dataDir = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    try {
      FileUtils.cleanDirectory(dataDir);
    } catch (IOException e) {
      // ignore
    }
    Assert.assertTrue(dataDir.isDirectory());
  }

  @Test (timeout = 10000)
  public void testSlaveRunnerStartStop() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestSlaveManager.TestSlaveManagerModule());
    ObjectGraph plus = objectGraph.plus(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    Manager manager = new SlaveManager(plus);
    manager.init();
    Runner runner = manager.getRunner("admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner.stop();
    while (runner.getStatus() != PipelineStatus.STOPPED) {
      Thread.sleep(100);
    }
  }

  @Test (timeout = 10000)
  public void testDisconnectingSlaveRunner() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestSlaveManager.TestSlaveManagerModule());
    ObjectGraph plus = objectGraph.plus(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    Manager manager = new SlaveManager(plus);
    manager.init();
    Runner runner = manager.getRunner("admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    runner.onDataCollectorStop();
    while (runner.getStatus() != PipelineStatus.DISCONNECTED) {
      Thread.sleep(100);
    }
  }

  @Test(timeout = 10000)
  public void testFinishedSlaveRunner() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestSlaveManager.TestSlaveManagerModule());
    ObjectGraph plus = objectGraph.plus(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    Manager manager = new SlaveManager(plus);
    manager.init();
    Runner runner = manager.getRunner("admin", TestUtil.MY_PIPELINE, "0");
    runner.start();
    while (runner.getStatus() != PipelineStatus.RUNNING) {
      Thread.sleep(100);
    }
    TestUtil.EMPTY_OFFSET = true;
    while (runner.getStatus() != PipelineStatus.FINISHED) {
      Thread.sleep(100);
    }
  }

}
