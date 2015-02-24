/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
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
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
                                                      boolean captureNextBatch, boolean sourceOffsetCommitter)
      throws PipelineRuntimeException, StageException {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    SourceOffsetTracker tracker = new TestUtil.SourceOffsetTrackerImpl("1");
    FileSnapshotStore snapshotStore = Mockito.mock(FileSnapshotStore.class);

    Mockito.when(snapshotStore.getSnapshotStatus(PIPELINE_NAME, REVISION)).thenReturn(new SnapshotStatus(false, false));
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    Configuration config = new Configuration();
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, snapshotStore,  deliveryGuarantee, PIPELINE_NAME, REVISION,
      new FilePipelineStoreTask(new RuntimeInfo(Arrays.asList(getClass().getClassLoader())), config) {
      }, productionObserveRequests, config);

    PipelineConfiguration pConf = (sourceOffsetCommitter)
        ? MockStages.createPipelineConfigurationSourceOffsetCommitterProcessorTarget()
        : MockStages.createPipelineConfigurationSourceProcessorTarget();

    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(), "name", pConf)
        .build(runner, tracker, null);

    if(captureNextBatch) {
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
      return Arrays.asList(context.createConfigIssue(null, null, ContainerError.CONTAINER_0000));
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

  public enum TestErrors implements ErrorCode {
    ERROR_S, ERROR_P;

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return "ERROR";
    }
  }

  private static class ErrorProducerSourceCapture extends BaseSource {
    public int count;
    public String offset;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      Record record = getContext().createRecord("e" + count);
      record.set(Field.create("E" + count));
      getContext().toError(record, TestErrors.ERROR_S);
      record.set(Field.create("OK" + count));
      batchMaker.addRecord(record);
      return (++count < 2) ? "o::" + count : null;
    }

  }

  private static class ErrorStageTarget extends BaseTarget {
    public List<Record> records = new ArrayList<>();

    @Override
    public void write(Batch batch) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        records.add(it.next());
      }
    }
  }

  private static class ErrorProducerProcessorCapture extends BaseProcessor {
    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
      Iterator<Record> it = batch.getRecords();
      while (it.hasNext()) {
        getContext().toError(it.next(), TestErrors.ERROR_P);
      }
    }
  }

  @Test
  public void testErrorStage() throws Exception {
    Source source = new ErrorProducerSourceCapture();
    MockStages.setSourceCapture(source);
    ErrorProducerProcessorCapture processor = new ErrorProducerProcessorCapture();
    MockStages.setProcessorCapture(processor);
    ErrorStageTarget errorStage = new ErrorStageTarget();
    MockStages.setErrorStageCapture(errorStage);
    ProductionPipeline pipeline = createProductionPipeline(DeliveryGuarantee.AT_MOST_ONCE, true, true);
    pipeline.run();
    Assert.assertEquals(4, errorStage.records.size());
    assertErrorRecord(errorStage.records.get(0), "id", PIPELINE_NAME, Field.create("E0"), TestErrors.ERROR_S.name(), "s");
    assertErrorRecord(errorStage.records.get(1), "id", PIPELINE_NAME, Field.create("OK0"), TestErrors.ERROR_P.name(), "p");
    assertErrorRecord(errorStage.records.get(2), "id", PIPELINE_NAME, Field.create("E1"), TestErrors.ERROR_S.name(), "s");
    assertErrorRecord(errorStage.records.get(3), "id", PIPELINE_NAME, Field.create("OK1"), TestErrors.ERROR_P.name(), "p");
  }

  private void assertErrorRecord(Record record, String sdcId, String pipelineName, Field field, String errorCode, String errorStage) {
    Assert.assertEquals(sdcId, record.getHeader().getErrorDataCollectorId());
    Assert.assertEquals(pipelineName, record.getHeader().getErrorPipelineName());
    Assert.assertEquals(field, record.get());
    Assert.assertEquals(errorCode, record.getHeader().getErrorCode());
    Assert.assertEquals(errorStage, record.getHeader().getErrorStage());
    Assert.assertNotNull(record.getHeader().getErrorMessage());
    Assert.assertNotEquals(0, record.getHeader().getErrorTimestamp());
  }
}
