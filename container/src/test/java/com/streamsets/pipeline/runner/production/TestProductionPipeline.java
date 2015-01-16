/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
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

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.stop();
    Assert.assertTrue(pipeline.wasStopped());

  }

  @Test
  public void testGetCommittedOffset() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals(null, pipeline.getCommittedOffset());
    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());

  }

  @Test
  public void testProductionRunnerOffsetAPIs() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, false, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertEquals("1", pipeline.getPipeline().getRunner().getSourceOffset());
    Assert.assertEquals(null, pipeline.getPipeline().getRunner().getNewSourceOffset());

  }

  @Test
  public void testProductionRunAtLeastOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_LEAST_ONCE, true, false);
    pipeline.run();

    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  @Test
  public void testProductionRunAtMostOnce() throws Exception {

    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, false);
    pipeline.run();
    //The source returns null offset the first time.
    Assert.assertNull(pipeline.getCommittedOffset());

    Assert.assertTrue(pipeline.getPipeline().getRunner().getBatchesOutput().isEmpty());
  }

  private static class SourceOffsetCommitterCapture implements Source, OffsetCommitter {
    public int count;
    public String offset;

    @Override
    public void commit(String offset) throws StageException {
      this.offset = offset;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return (count++ == 0) ? "x" : null;
    }

    @Override
    public void init(Info info, Context context) throws StageException {

    }

    @Override
    public void destroy() {

    }
  }

  @Test
  public void testProductionRunWithSourceOffsetCommitter() throws Exception {
    SourceOffsetCommitterCapture capture = new SourceOffsetCommitterCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.run();
    Assert.assertEquals("x", capture.offset);
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee,
                                                      boolean capturenextBatch, boolean sourceOffsetCommitter)
      throws PipelineRuntimeException {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);
    FileErrorRecordStore fileErrorRecordStore = Mockito.mock(FileErrorRecordStore.class);

    Mockito.when(snapshotStore.getSnapshotStatus(PIPELINE_NAME, REVISION)).thenReturn(new SnapshotStatus(false, false));
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore, fileErrorRecordStore, 5
        , 10, 10, deliveryGuarantee, PIPELINE_NAME, REVISION);

    PipelineConfiguration pConf = (sourceOffsetCommitter)
        ? MockStages.createPipelineConfigurationSourceOffsetCommitterProcessorTarget()
        : MockStages.createPipelineConfigurationSourceProcessorTarget();

    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name", pConf)
        .build(runner, tracker);

    if(capturenextBatch) {
      runner.captureNextBatch(1);
    }

    return pipeline;
  }


}
