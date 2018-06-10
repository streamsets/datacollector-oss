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
package com.streamsets.datacollector.execution.runner;

import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.StartPipelineContextBuilder;
import com.streamsets.datacollector.execution.manager.TestSlaveManager;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.util.TestUtil.TestPipelineProviderModule;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static com.streamsets.datacollector.util.AwaitConditionUtil.desiredPipelineState;
import static org.awaitility.Awaitility.await;

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
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    ObjectGraph plus = objectGraph.plus(new TestSlaveManager.TestSlaveManagerModule());

    Manager manager = new SlavePipelineManager(plus);
    manager.init();
    Runner runner = manager.getRunner(TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    await().until(desiredPipelineState(runner, PipelineStatus.RUNNING));
    runner.stop("admin");
    await().until(desiredPipelineState(runner, PipelineStatus.STOPPED));
  }

  @Test(timeout = 20000)
  public void testLoadingUnsupportedPipeline() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    ObjectGraph plus = objectGraph.plus(new TestSlaveManager.TestSlaveManagerModule());
    Manager pipelineManager = new SlavePipelineManager(plus);
    pipelineManager.init();
    Runner runner = pipelineManager.getRunner(TestUtil.HIGHER_VERSION_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    final Runner newerPipelineRunner = pipelineManager.getRunner(TestUtil.HIGHER_VERSION_PIPELINE, "0");
    await().until(desiredPipelineState(newerPipelineRunner, PipelineStatus.START_ERROR));
    PipelineState state = newerPipelineRunner.getState();
    Assert.assertTrue(state.getStatus() == PipelineStatus.START_ERROR);
    Assert.assertTrue(state.getMessage().contains("CONTAINER_0158"));
  }

  @Test (timeout = 10000)
  public void testDisconnectingSlaveRunner() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    ObjectGraph plus = objectGraph.plus(new TestSlaveManager.TestSlaveManagerModule());
    Manager manager = new SlavePipelineManager(plus);
    manager.init();
    Runner runner = manager.getRunner(TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    await().until(desiredPipelineState(runner, PipelineStatus.RUNNING));
    runner.onDataCollectorStop("admin");
    await().until(desiredPipelineState(runner, PipelineStatus.DISCONNECTED));
  }

  @Test(timeout = 10000)
  public void testFinishedSlaveRunner() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineProviderModule(TestUtil.MY_PIPELINE, "0"));
    ObjectGraph plus = objectGraph.plus(new TestSlaveManager.TestSlaveManagerModule());
    Manager manager = new SlavePipelineManager(plus);
    manager.init();
    Runner runner = manager.getRunner(TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    await().until(desiredPipelineState(runner, PipelineStatus.RUNNING));
    TestUtil.EMPTY_OFFSET = true;
    await().until(desiredPipelineState(runner, PipelineStatus.FINISHED));
  }

}
