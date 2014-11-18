/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.production.*;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Arrays;

public class TestPipelineManagerTask {

  private PipelineProductionManagerTask manager = null;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("pipeline.data.dir", "./target/var");
    TestUtil.captureMockStages();
  }

  @Before()
  public void setUp() throws PipelineStoreException {
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.when(configuration.get("maxBatchSize", 10)).thenReturn(10);
    FilePipelineStoreTask filePipelineStoreTask = Mockito.mock(FilePipelineStoreTask.class);
    Mockito.when(filePipelineStoreTask.load("xyz", "1.0")).thenReturn(Mockito.mock(PipelineConfiguration.class));

    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);

    manager = new PipelineProductionManagerTask(info, configuration
        , filePipelineStoreTask, stageLibraryTask);
    manager.init();
  }

  @After
  public void tearDown() {
    manager.stop();
    manager.getStateTracker().getStateFile().delete();
    manager.getOffsetTracker().getOffsetFile().delete();
  }

  @Test
  public void testGetAndSetPipelineState() throws PipelineStateException {
    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());
    manager.setState("1.0", State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    Assert.assertEquals("Started Running", manager.getPipelineState().getMessage());

    manager.setState("1.0", State.ERROR, "Error");
    Assert.assertEquals(State.ERROR, manager.getPipelineState().getState());
    Assert.assertEquals("Error", manager.getPipelineState().getMessage());
  }

  @Test
  public void testSetOffset() throws PipelineStateException {
    Assert.assertNull(manager.getOffsetTracker().getOffset());
    manager.setOffset("abc");
    Assert.assertNotNull(manager.getOffsetTracker().getOffset());
    Assert.assertEquals("abc", manager.getOffsetTracker().getOffset());
  }

  @Test(expected = PipelineStateException.class)
  public void testSetOffsetWhenRunning() throws PipelineStateException, StageException, PipelineRuntimeException, PipelineStoreException {
    manager.setState("1.0", State.RUNNING, "Started Running");
    manager.setOffset("abc");
  }


  @Test
  public void testSnapshotStatus() {
    SnapshotStatus snapshotStatus = manager.snapshotStatus();
    Assert.assertEquals(false, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

  }

  @Test(expected = PipelineStateException.class)
  public void testStartPipelineWhenRunning() throws PipelineStateException, StageException, PipelineRuntimeException, PipelineStoreException {

    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());
    manager.setState("1.0", State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.startPipeline("1.0");
  }

  @Test(expected = PipelineStateException.class)
  public void testStopPipelineWhenNotRunning() throws PipelineStateException, StageException, PipelineRuntimeException, PipelineStoreException {
    manager.stopPipeline();
  }

  /*@Test
  public void testStartPipeline() throws PipelineStoreException, PipelineStateException, PipelineRuntimeException, StageException {
    manager.startPipeline("1.0");

  }*/

  /*

  @Test
  public void testRunningToError() throws PipelineStateException {

    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());

    manager.setState("1.0", State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    Assert.assertEquals("Started Running", manager.getPipelineState().getMessage());

    manager.setState("1.0", State.ERROR, "Error");
    Assert.assertEquals(State.ERROR, manager.getPipelineState().getState());
    Assert.assertEquals("Error", manager.getPipelineState().getMessage());

  }

  @Test
  public void testSuccessState() throws PipelineRuntimeException {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(FileSnapshotStore.class),tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, "1.0");

    //state should be not running
    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());

    //Stops after the first batch
    runnable.run();

    Assert.assertTrue(manager.getPipelineState() != null);
    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());
    Assert.assertEquals("Completed successfully.", manager.getPipelineState().getMessage());
    //Offset 1 expected as there was a Runtime exception
    Assert.assertEquals(null, pipeline.getCommittedOffset());
    //no output as captured as there was an exception in source
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());

  }

  @Test
  public void testErrorState() throws PipelineRuntimeException {
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        throw new RuntimeException("Simulate runtime failure in source");
      }
    });

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(FileSnapshotStore.class),tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline, "1.0");
    runner.captureNextBatch(1);

    //state should be not running

    Assert.assertEquals(State.NOT_RUNNING, manager.getPipelineState().getState());
    runnable.run();

    Assert.assertTrue(manager.getPipelineState() != null);
    Assert.assertEquals(State.ERROR, manager.getPipelineState().getState());
    Assert.assertTrue(manager.getPipelineState().getMessage() != null && !manager.getPipelineState().getMessage().isEmpty());
    //Offset 1 expected as there was a Runtime exception
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as captured as there was an exception in source
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());
  }*/

  @Test(expected = PipelineStateException.class)
  public void testCaptureSnapshot() throws PipelineStateException {
    //cannot capture snapshot when pipeline is not running
    manager.captureSnapshot(10);
  }

  @Test(expected = PipelineStateException.class)
  public void testCaptureSnapshotInvalidBatch() throws PipelineStateException {
    //cannot capture snapshot when pipeline is not running
    manager.setState("1.0", State.RUNNING, "Started Running");
    manager.captureSnapshot(0);
  }

}
