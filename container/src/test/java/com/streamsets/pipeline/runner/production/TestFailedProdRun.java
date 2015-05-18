/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.SourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestFailedProdRun {

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
  }

  private static final String PIPELINE_NAME = "xyz";
  private static final String REVISION = "0";

  @Test(expected = PipelineRuntimeException.class)
  public void testPipelineOpenLanes() throws PipelineRuntimeException, StageException {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    MockStages.setSourceCapture(new BaseSource() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        Record record = getContext().createRecord("x");
        record.set(Field.create(1));
        batchMaker.addRecord(record);
        return "1";
      }
    });
    MockStages.setProcessorCapture(new SingleLaneRecordProcessor() {
      @Override
      protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        record.set(Field.create(2));
        batchMaker.addRecord(record);
      }
    });
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, Mockito.mock(FileSnapshotStore.class),
      DeliveryGuarantee.AT_MOST_ONCE, PIPELINE_NAME, REVISION, productionObserveRequests, new Configuration(),
      new MemoryLimitConfiguration());
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);

    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(),
        PIPELINE_NAME, REVISION, runtimeInfo, pipelineConfiguration).build(runner, tracker, null);

  }


  private static class ErrorListeningSource extends BaseSource implements ErrorListener {
    static Throwable capturedError;
    static RuntimeException thrownError;

    @Override
    public void errorNotification(Throwable throwable) {
      capturedError = throwable;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      if (thrownError != null) {
        throw thrownError;
      }
      return null;
    }
  }

  static class SomeException extends RuntimeException {
    SomeException(String msg) {
      super(msg);
    }
  }

  @Test
  public void testPipelineError() throws PipelineRuntimeException, StageException {
    String msg = "ERROR YALL";
    ErrorListeningSource.thrownError = new SomeException(msg);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    MockStages.setSourceCapture(new ErrorListeningSource());
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, Mockito.mock(FileSnapshotStore.class),
      DeliveryGuarantee.AT_MOST_ONCE, PIPELINE_NAME, REVISION, productionObserveRequests, new Configuration(),
      new MemoryLimitConfiguration());
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    ProductionPipeline pipeline = new ProductionPipelineBuilder(MockStages.createStageLibrary(),
      PIPELINE_NAME, REVISION, runtimeInfo, pipelineConfiguration).build(runner, tracker, null);
    try {
      pipeline.run();
    } catch (SomeException ex) {
      Assert.assertSame(ex, ErrorListeningSource.thrownError);
    }
    Assert.assertSame(ErrorListeningSource.thrownError, ErrorListeningSource.capturedError);
    Assert.assertEquals(msg, ErrorListeningSource.capturedError.getMessage());
  }

}
