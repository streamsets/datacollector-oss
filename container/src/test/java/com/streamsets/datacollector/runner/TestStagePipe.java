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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.util.Collections;
import java.util.List;

public class TestStagePipe {
  private boolean produce;
  private boolean process;
  private boolean write;

  private EventSink eventSink;
  private ErrorSink errorSink;

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
    this.eventSink = new EventSink();
    this.errorSink = new ErrorSink();
    ImmutableList.of("e", "s", "p", "t").forEach(instanceName -> {
      this.eventSink.registerInterceptorsForStage(instanceName, Collections.emptyList());
      this.errorSink.registerInterceptorsForStage(instanceName, Collections.emptyList());
    });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSourceStage() throws Exception {
    produce = false;
    MockStages.setSourceCapture(new Source() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        produce = true;
        Assert.assertEquals("offset1", lastSourceOffset);
        Assert.assertEquals(ImmutableList.of("s"), batchMaker.getLanes());
        return "offset2";
      }

      @Override
      public List<ConfigIssue> init(Info info, Context context) {
        return Collections.emptyList();
      }

      @Override
      public void destroy() {

      }
    });
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
     Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceProcessorTarget())
      .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getSourcePipe();

    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableList.of("s"));
    Mockito.when(batchMaker.getSize()).thenReturn(1);

    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    Mockito.when(pipeBatch.getPreviousOffset()).thenReturn("offset1");

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSize()).thenReturn(1);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);

    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Assert.assertTrue(pipe.init(new PipeContext()).isEmpty());

    pipe.process(pipeBatch);
    StagePipe.Context stagePipeContext = Whitebox.getInternalState(pipe, "context");
    long timeOfLastReceivedRecordBeforeEmptyBatch = stagePipeContext.getRuntimeStats().getTimeOfLastReceivedRecord();
    long batchCountBeforeEmptyBatch = stagePipeContext.getRuntimeStats().getBatchCount();
    pipe.destroy(pipeBatch);

    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatchSize();
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.any(Pipe.class));
    Mockito.verify(pipeBatch, Mockito.times(1)).getPreviousOffset();
    Mockito.verify(pipeBatch, Mockito.times(1)).setNewOffset(Mockito.eq("offset2"));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.any(StagePipe.class));
    Mockito.verify(pipeBatch, Mockito.times(2)).getErrorSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getEventSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getProcessedSink();
    Mockito.verify(pipeBatch, Mockito.times(1)).getSourceResponseSink();
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(produce);

    //Check last received record for empty batch
    batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSize()).thenReturn(0);
    //empty batch
    Mockito.when(batch.getRecords()).thenReturn(Collections.emptyIterator());
    Mockito.when(batchMaker.getSize()).thenReturn(0);

    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);

    batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableList.of("s"));

    pipeBatch = Mockito.mock(FullPipeBatch.class);
    Mockito.when(pipeBatch.getPreviousOffset()).thenReturn("offset1");

    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);
    pipe.process(pipeBatch);

    pipe = (StagePipe) pipeline.getSourcePipe();
    long timeOfLastReceivedRecordAfterEmptyBatch = stagePipeContext.getRuntimeStats().getTimeOfLastReceivedRecord();
    long batchCountAfterEmptyBatch = stagePipeContext.getRuntimeStats().getBatchCount();

    Assert.assertEquals(timeOfLastReceivedRecordBeforeEmptyBatch, timeOfLastReceivedRecordAfterEmptyBatch);
    Assert.assertEquals(batchCountBeforeEmptyBatch + 1, batchCountAfterEmptyBatch);

    pipe.destroy(pipeBatch);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws Exception {
    process = false;
    MockStages.setProcessorCapture(new Processor() {

      @Override
      public List<ConfigIssue> init(Info info, Context context) {
        return Collections.emptyList();
      }

      @Override
      public void process(Batch batch, BatchMaker batchMaker) throws StageException {
        process = true;
        Assert.assertEquals("offset2", batch.getSourceOffset());
        Assert.assertEquals(ImmutableList.of("p"), batchMaker.getLanes());
      }

      @Override
      public void destroy() {
      }
    });
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceProcessorTarget())
      .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getRunners().get(0).get(2);
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableList.of("p"));

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSourceOffset()).thenReturn("offset2");
    Mockito.when(batch.getSize()).thenReturn(1);

    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);

    Assert.assertTrue(pipe.init(new PipeContext()).isEmpty());
    pipe.process(pipeBatch);
    long timeOfLastReceivedRecordBeforeEmptyBatch =
        ((StagePipe.Context)Whitebox.getInternalState(pipe, "context"))
            .getRuntimeStats().getTimeOfLastReceivedRecord();

    pipe.destroy(pipeBatch);

    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getPreviousOffset();
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatchSize();
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.any(StagePipe.class));
    Mockito.verify(pipeBatch, Mockito.times(2)).getErrorSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getEventSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getProcessedSink();
    Mockito.verify(pipeBatch, Mockito.times(1)).getSourceResponseSink();
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(process);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTarget() throws Exception {
    write = false;
    MockStages.setTargetCapture(new Target() {

      @Override
      public List<ConfigIssue> init(Info info, Context context) {
        return Collections.emptyList();
      }

      @Override
      public void write(Batch batch) throws StageException {
        write = true;
        Assert.assertEquals("offset2", batch.getSourceOffset());
      }

      @Override
      public void destroy() {
      }
    });

    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceProcessorTarget())
      .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getRunners().get(0).get(5);
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableList.of("t"));

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSourceOffset()).thenReturn("offset2");
    Mockito.when(batch.getSize()).thenReturn(1);

    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);

    Assert.assertTrue(pipe.init(new PipeContext()).isEmpty());
    pipe.process(pipeBatch);
    pipe.destroy(pipeBatch);

    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getPreviousOffset();
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatchSize();
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.any(StagePipe.class));
    Mockito.verify(pipeBatch, Mockito.times(2)).getErrorSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getEventSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getProcessedSink();
    Mockito.verify(pipeBatch, Mockito.times(1)).getSourceResponseSink();
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(write);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecutor() throws Exception {
    write = false;
    MockStages.setExecutorCapture(new Executor() {

      @Override
      public List<ConfigIssue> init(Info info, Context context) {
        return Collections.emptyList();
      }

      @Override
      public void write(Batch batch) throws StageException {
        write = true;
        Assert.assertEquals("offset2", batch.getSourceOffset());
      }

      @Override
      public void destroy() {
      }
    });

    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceTargetWithEventsProcessed())
      .build(pipelineRunner);
    final int index = 2;
    Assert.assertEquals("executorName", pipeline.getRunners().get(0).get(index).getStage().getDefinition().getName());
    Assert.assertTrue(pipeline.getRunners().get(0).get(index) instanceof StagePipe);
    StagePipe pipe = (StagePipe) pipeline.getRunners().get(0).get(index);
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableList.of("t"));

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSourceOffset()).thenReturn("offset2");
    Mockito.when(batch.getSize()).thenReturn(1);

    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);
    Mockito.when(pipeBatch.getErrorSink()).thenReturn(errorSink);
    Mockito.when(pipeBatch.getEventSink()).thenReturn(eventSink);

    Assert.assertTrue(pipe.init(new PipeContext()).isEmpty());
    pipe.process(pipeBatch);
    pipe.destroy(pipeBatch);

    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getPreviousOffset();
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatchSize();
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.any(StagePipe.class));
    Mockito.verify(pipeBatch, Mockito.times(2)).getErrorSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getEventSink();
    Mockito.verify(pipeBatch, Mockito.times(2)).getProcessedSink();
    Mockito.verify(pipeBatch, Mockito.times(1)).getSourceResponseSink();
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(write);
  }
}
