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
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.state.PipelineManagerTask;
import com.streamsets.pipeline.state.State;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.*;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class TestProdPipelineRunnable {

  private PipelineManagerTask manager = null;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("pipeline.data.dir", "./target/var");
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
  public void testRun() throws PipelineRuntimeException {

    TestUtil.captureMockStages();

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(SnapshotPersister.class), tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline);
    runner.captureNextBatch(1);
    runnable.run();

    //The source returns null offset because all the data from source was read
    Assert.assertNull(pipeline.getCommittedOffset());

    //output expected as capture snapshot was set to true
    List<StageOutput> output = runner.getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).getField("f").getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).getField("f").getValue());
  }

  @Test
  public void testStop() throws PipelineRuntimeException {

    TestUtil.captureMockStages();

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(SnapshotPersister.class), tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline);

    runnable.stop();
    Assert.assertTrue(pipeline.wasStopped());

    //Stops after the first batch
    runnable.run();

    //Offset 1 expected as pipeline was stopped after the first batch
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as capture was not set to true
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());
  }

  @Test
  public void testException() throws PipelineRuntimeException {
    System.setProperty("pipeline.data.dir", "./target/var");

    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        throw new RuntimeException("Simulate runtime failure in source");
      }
    });

    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(SnapshotPersister.class), tracker, 5
        , DeliveryGuarantee.AT_MOST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    ProductionPipelineRunnable runnable = new ProductionPipelineRunnable(manager, pipeline);
    runner.captureNextBatch(1);
    //Stops after the first batch
    runnable.run();

    Assert.assertEquals(State.ERROR, manager.getState().getPipelineState());
    //Offset 1 expected as there was a Runtime exception
    Assert.assertEquals("1", pipeline.getCommittedOffset());
    //no output as captured as there was an exception in source
    Assert.assertTrue(runner.getBatchesOutput().isEmpty());
  }

}
