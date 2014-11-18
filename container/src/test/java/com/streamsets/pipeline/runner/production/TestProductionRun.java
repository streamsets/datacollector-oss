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

import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.runner.*;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;


public class TestProductionRun {

  @BeforeClass
  public static void beforeClass() {
    TestUtil.captureMockStages();
  }

  @Test
  public void testStopPipeline() throws Exception {

    ProductionPipeline pipeline = createPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.stop();
    Assert.assertTrue(pipeline.wasStopped());

  }

  @Test
  public void testGetCommittedOffset() throws Exception {

    ProductionPipeline pipeline = createPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals(null, pipeline.getCommittedOffset());
    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());

  }

  @Test
  public void testProductionRunnerOffsetAPIs() throws Exception {

    ProductionPipeline pipeline = createPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals("1", pipeline.getPipeline().getRunner().getSourceOffset());
    Assert.assertEquals(null, pipeline.getPipeline().getRunner().getNewSourceOffset());

  }

  @Test
  public void testProductionRunAtLeastOnce() throws Exception {

    ProductionPipeline pipeline = createPipeline(DeliveryGuarantee.AT_LEAST_ONCE, true);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    List<StageOutput> output = pipeline.getPipeline().getRunner().getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).getField("f").getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).getField("f").getValue());
  }

  @Test
  public void testProductionRunAtMostOnce() throws Exception {

    ProductionPipeline pipeline = createPipeline(DeliveryGuarantee.AT_MOST_ONCE, true);
    pipeline.run();
    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    List<StageOutput> output = pipeline.getPipeline().getRunner().getBatchesOutput().get(0);
    Assert.assertEquals(1, output.get(0).getOutput().get("s").get(0).getField("f").getValue());
    Assert.assertEquals(2, output.get(1).getOutput().get("p").get(0).getField("f").getValue());
  }

  private ProductionPipeline createPipeline(DeliveryGuarantee deliveryGuarantee, boolean captureNextBatch)
      throws PipelineRuntimeException {
    SourceOffsetTracker mockTracker = new TestUtil.SourceOffsetTrackerImpl("1");
    ProductionPipelineRunner runner = new ProductionPipelineRunner(Mockito.mock(FileSnapshotStore.class),mockTracker, 5
        , DeliveryGuarantee.AT_LEAST_ONCE);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);
    if(captureNextBatch) {
      runner.captureNextBatch(1);
    }
    return pipeline;
  }


}
