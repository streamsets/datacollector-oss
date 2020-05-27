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
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.runner.common.TestProductionPipeline.MyStateListener;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotInfoImpl;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;

import dagger.ObjectGraph;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class TestProdPipelineRunnable {

  private static final String SNAPSHOT_NAME = "snapshot";
  private Runner runner;
  private PipelineStateStore pipelineStateStore;
  private Manager manager;
  private File testDir;

  @Before()
  public void setUp() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
    MockStages.resetStageCaptures();
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    manager = new StandaloneAndClusterPipelineManager(objectGraph);
    manager.init();
    manager.run();
    runner = manager.getRunner(TestUtil.MY_PIPELINE, "0");
  }

  @After
  public void tearDown() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testRun() throws Exception {
    TestUtil.captureMockStages();
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true);
    pipeline.registerStatusListener(new MyStateListener());
    ProductionPipelineRunnable runnable =
      new ProductionPipelineRunnable(null, runner.getRunner(StandaloneRunner.class), pipeline, TestUtil.MY_PIPELINE, "0",
        Collections.<Future<?>> emptyList());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING, null, null, null, null, 0, 0);
    runnable.run();
    // The source returns null offset because all the data from source was read
    Assert.assertTrue(pipeline.getCommittedOffsets().isEmpty());
  }

  @Test
  public void testStop() throws Exception {
    TestUtil.captureMockStages();
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, false);
    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable
      (null, runner.getRunner(StandaloneRunner.class), pipeline, TestUtil.MY_PIPELINE, "0",
      Collections.<Future<?>>emptyList());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING, null, null, null, null, 0, 0);
    //Stops after the first batch
    runnable.run();
    runnable.stop(false);
    Assert.assertTrue(pipeline.wasStopped());
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch)
    throws StageException, PipelineException {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl(Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "1"));
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);

    Mockito.when(snapshotStore.getInfo(TestUtil.MY_PIPELINE, "0", SNAPSHOT_NAME)).
      thenReturn(new SnapshotInfoImpl("user", SNAPSHOT_NAME, "SNAPSHOT LABEL", TestUtil.MY_PIPELINE, "0",
          System.currentTimeMillis(), false, 0, false));
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    Configuration conf = new Configuration();
    ProductionPipelineRunner runner =
      new ProductionPipelineRunner(
          TestUtil.MY_PIPELINE,
          "0",
          null,
          conf,
          buildInfo,
          runtimeInfo,
          new MetricRegistry(),
          snapshotStore,
          null,
          null
      );
    runner.setDeliveryGuarantee(deliveryGuarantee);
    runner.setObserveRequests(productionObserveRequests);
    runner.setOffsetTracker(tracker);

    ProductionPipeline pipeline = new ProductionPipelineBuilder(
      TestUtil.MY_PIPELINE,
      "0",
      conf,
      runtimeInfo,
      buildInfo,
      MockStages.createStageLibrary(),
      runner,
      null,
      Mockito.mock(BlobStoreTask.class),
      Mockito.mock(LineagePublisherTask.class),
      Mockito.mock(StatsCollector.class)
    ).build(
      MockStages.userContext(),
      MockStages.createPipelineConfigurationSourceProcessorTarget(),
      System.currentTimeMillis()
    );

    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPED, null, null, null, null, 0, 0);

    if(captureNextBatch) {
      runner.capture(SNAPSHOT_NAME, 1, 1);
    }

    return pipeline;
  }

}
