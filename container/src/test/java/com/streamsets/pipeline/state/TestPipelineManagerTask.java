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
package com.streamsets.pipeline.state;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.production.*;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Arrays;

public class TestPipelineManagerTask {

  private PipelineManagerTask manager = null;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("pipeline.data.dir", "./target/var");
    TestUtil.captureMockStages();
  }

  @Before()
  public void setUp() {
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    manager = new PipelineManagerTask(info);
    manager.init();
  }

  @After
  public void tearDown() {
    manager.stop();
    manager.getStateTracker().getStateFile().delete();
  }

  @Test
  public void testNotRunningToRunning() throws PipelineStateException {

    Assert.assertEquals(State.NOT_RUNNING, manager.getState().getPipelineState());

    manager.setState(State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getState().getPipelineState());
    Assert.assertEquals("Started Running", manager.getState().getMessage());

    manager.setState(State.ERROR, "Error");
    Assert.assertEquals(State.ERROR, manager.getState().getPipelineState());
    Assert.assertEquals("Error", manager.getState().getMessage());

  }

  @Test
  public void testRunningToError() throws PipelineStateException {

    Assert.assertEquals(State.NOT_RUNNING, manager.getState().getPipelineState());

    manager.setState(State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getState().getPipelineState());
    Assert.assertEquals("Started Running", manager.getState().getMessage());

    manager.setState(State.ERROR, "Error");
    Assert.assertEquals(State.ERROR, manager.getState().getPipelineState());
    Assert.assertEquals("Error", manager.getState().getMessage());

  }

  @Test
  public void testSuccessState() throws PipelineRuntimeException {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(SnapshotPersister.class),tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline);

    //state should be not running
    Assert.assertEquals(State.NOT_RUNNING, manager.getState().getPipelineState());

    //Stops after the first batch
    runnable.run();

    Assert.assertTrue(manager.getState() != null);
    Assert.assertEquals(State.NOT_RUNNING, manager.getState().getPipelineState());
    Assert.assertEquals("Completed successfully.", manager.getState().getMessage());
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
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(SnapshotPersister.class),tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline);
    runner.captureNextBatch(1);

    //state should be not running

    Assert.assertEquals(State.NOT_RUNNING, manager.getState().getPipelineState());
    runnable.run();

    Assert.assertTrue(manager.getState() != null);
    Assert.assertEquals(State.ERROR, manager.getState().getPipelineState());
    Assert.assertTrue(manager.getState().getMessage() != null && !manager.getState().getMessage().isEmpty());
    //Offset 1 expected as there was a Runtime exception
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as captured as there was an exception in source
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());
  }

  @Test(expected = PipelineStateException.class)
  public void testCaptureSnapshot() throws PipelineStateException {
    //cannot capture snapshot when pipeline is not running
    manager.captureSnapshot(10);
  }

}
