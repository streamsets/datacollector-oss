/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.errorrecordstore.impl.FileErrorRecordStore;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestProductionPipeline {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "0";

  @BeforeClass
  public static void beforeClass() {
    TestUtil.captureMockStages();
  }

  @Test
  public void testStopPipeline() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.stop();
    Assert.assertTrue(pipeline.wasStopped());

  }

  @Test
  public void testGetCommittedOffset() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals(null, pipeline.getCommittedOffset());
    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());

  }

  @Test
  public void testProductionRunnerOffsetAPIs() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals("1", pipeline.getPipeline().getRunner().getSourceOffset());
    Assert.assertEquals(null, pipeline.getPipeline().getRunner().getNewSourceOffset());

  }

  @Test
  public void testProductionRunAtLeastOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, true);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  @Test
  public void testProductionRunAtMostOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true);
    pipeline.run();
    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee,
                                                      boolean capturenextBatch) throws PipelineRuntimeException {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);
    FileErrorRecordStore fileErrorRecordStore = Mockito.mock(FileErrorRecordStore.class);

    Mockito.when(snapshotStore.getSnapshotStatus(PIPELINE_NAME)).thenReturn(new SnapshotStatus(false, false));
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore, fileErrorRecordStore, tracker, 5
        , 10, 10, deliveryGuarantee, PIPELINE_NAME, REVISION);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name",
        MockStages.createPipelineConfigurationSourceProcessorTarget()).build(runner);

    if(capturenextBatch) {
      runner.captureNextBatch(1);
    }

    return pipeline;
  }


}
