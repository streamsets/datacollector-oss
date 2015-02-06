/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.errorrecordstore.impl.FileErrorRecordStore;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestProductionPipeline {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String REVISION = "0";

  @Before
  public void beforeClass() {
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
    public List<ConfigIssue> validateConfigs(Info info, Context context) {
      return Collections.emptyList();
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

  public static class PreviewCheckSource extends BaseSource {
    public boolean isPreview;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      isPreview  = getContext().isPreview();
      return null;
    }
  }

  @Test
  public void testIsPreview() throws Exception {
    PreviewCheckSource capture = new PreviewCheckSource();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.run();
    Assert.assertFalse(capture.isPreview);
  }

  private ProductionPipeline createProductionPipeline(DeliveryGuarantee deliveryGuarantee,
                                                      boolean capturenextBatch, boolean sourceOffsetCommitter)
      throws PipelineRuntimeException {
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);
    FileErrorRecordStore fileErrorRecordStore = Mockito.mock(FileErrorRecordStore.class);

    Mockito.when(snapshotStore.getSnapshotStatus(PIPELINE_NAME, REVISION)).thenReturn(new SnapshotStatus(false, false));
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore, fileErrorRecordStore, 5
      , 10, 10, deliveryGuarantee, PIPELINE_NAME, REVISION,
      new FilePipelineStoreTask(new RuntimeInfo(Arrays.asList(getClass().getClassLoader())), new Configuration()) {
      });

    PipelineConfiguration pConf = (sourceOffsetCommitter)
        ? MockStages.createPipelineConfigurationSourceOffsetCommitterProcessorTarget()
        : MockStages.createPipelineConfigurationSourceProcessorTarget();

    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name", pConf)
        .build(runner, tracker, null);

    if(capturenextBatch) {
      runner.captureNextBatch(1);
    }

    return pipeline;
  }

  private static class SourceValidateConfigFailureCapture implements Source {
    public int count;
    public String offset;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return (count++ == 0) ? "x" : null;
    }

    @Override
    public List<ConfigIssue> validateConfigs(Info info, Context context) {
      return Arrays.asList(context.createConfigIssue(ContainerError.CONTAINER_0000));
    }

    @Override
    public void init(Info info, Context context) throws StageException {

    }

    @Override
    public void destroy() {

    }
  }


  @Test(expected = PipelineRuntimeException.class)
  public void testProductionRunWithFailedValidateConfigs() throws Exception {
    Source capture = new SourceValidateConfigFailureCapture();
    MockStages.setSourceCapture(capture);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.run();
  }

}
