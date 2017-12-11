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
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.util.Configuration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
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
    Mockito.when(runtimeInfo.getDataDir()).thenReturn("target/" + UUID.randomUUID());
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
    Configuration conf = new Configuration();
    ProductionPipelineRunner runner = new ProductionPipelineRunner(PIPELINE_NAME, REVISION, null, conf, runtimeInfo,
      new MetricRegistry(), Mockito.mock(FileSnapshotStore.class), null);
    runner.setMemoryLimitConfiguration(new MemoryLimitConfiguration());
    runner.setObserveRequests(productionObserveRequests);
    runner.setOffsetTracker(tracker);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    pipelineConfiguration.getStages().remove(2);
    ProductionPipeline pipeline = new ProductionPipelineBuilder(
      PIPELINE_NAME,
      REVISION,
      conf,
      runtimeInfo,
      MockStages.createStageLibrary(),
      runner,
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(
      MockStages.userContext(),
      pipelineConfiguration,
      System.currentTimeMillis()
    );


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
    Mockito.when(runtimeInfo.getDataDir()).thenReturn("target/" + UUID.randomUUID());
    MockStages.setSourceCapture(new ErrorListeningSource());
    SourceOffsetTracker tracker = Mockito.mock(SourceOffsetTracker.class);
    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(100, true /*FIFO*/);
    Configuration conf = new Configuration();
    ProductionPipelineRunner runner = new ProductionPipelineRunner(PIPELINE_NAME, REVISION, null, conf, runtimeInfo, new MetricRegistry(), Mockito.mock(FileSnapshotStore.class),
      null);
    runner.setMemoryLimitConfiguration(new MemoryLimitConfiguration());
    runner.setObserveRequests(productionObserveRequests);
    runner.setOffsetTracker(tracker);
    PipelineConfiguration pipelineConfiguration = MockStages.createPipelineConfigurationSourceProcessorTarget();
    ProductionPipeline pipeline = new ProductionPipelineBuilder(
      PIPELINE_NAME,
      REVISION,
      conf,
      runtimeInfo,
      MockStages.createStageLibrary(),
      runner,
      null,
      Mockito.mock(LineagePublisherTask.class)
    ).build(
      MockStages.userContext(),
      pipelineConfiguration,
      System.currentTimeMillis()
    );
    try {
      pipeline.registerStatusListener(new TestProductionPipeline.MyStateListener());
      pipeline.run();
    } catch (SomeException ex) {
      Assert.assertSame(ex, ErrorListeningSource.thrownError);
    }
    Assert.assertSame(ErrorListeningSource.thrownError, ErrorListeningSource.capturedError);
    Assert.assertTrue(ErrorListeningSource.capturedError.toString().endsWith(msg));
  }

}
