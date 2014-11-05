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
package com.streamsets.pipeline.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStagePipe {
  private boolean produce;
  private boolean process;
  private boolean write;

  @Test
  @SuppressWarnings("unchecked")
  public void testSourceStage() throws Exception {
    produce = false;
    MockStages.setSourceCapture(new Source() {
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        produce = true;
        Assert.assertEquals("offset1", lastSourceOffset);
        Assert.assertEquals(ImmutableSet.of("s"), batchMaker.getLanes());
        return "offset2";
      }

      @Override
      public void init(Info info, Context context) throws StageException {
      }

      @Override
      public void destroy() {
      }
    });
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(),
                                             MockStages.createPipelineConfigurationSourceProcessorTarget())
        .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getPipes()[0];
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableSet.of("s"));

    PipeBatch pipeBatch = Mockito.mock(PipeBatch.class);
    Mockito.when(pipeBatch.getPreviousOffset()).thenReturn("offset1");
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatchSize();
    Mockito.verify(pipeBatch, Mockito.times(1)).getPreviousOffset();
    Mockito.verify(pipeBatch, Mockito.times(1)).setNewOffset(Mockito.eq("offset2"));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(produce);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws Exception {
    process = false;
    MockStages.setProcessorCapture(new Processor() {

      @Override
      public void process(Batch batch, BatchMaker batchMaker) throws StageException {
        process = true;
        Assert.assertEquals("offset2", batch.getSourceOffset());
        Assert.assertEquals(ImmutableSet.of("p"), batchMaker.getLanes());
      }

      @Override
      public void init(Info info, Context context) throws StageException {
      }

      @Override
      public void destroy() {
      }
    });
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(),
                                             MockStages.createPipelineConfigurationSourceProcessorTarget())
        .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getPipes()[4];
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableSet.of("p"));

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSourceOffset()).thenReturn("offset2");

    PipeBatch pipeBatch = Mockito.mock(PipeBatch.class);
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);

    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(process);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTarget() throws Exception {
    write = false;
    MockStages.setTargetCapture(new Target() {
      @Override
      public void write(Batch batch) throws StageException {
        write = true;
        Assert.assertEquals("offset2", batch.getSourceOffset());
      }

      @Override
      public void init(Info info, Context context) throws StageException {
      }

      @Override
      public void destroy() {
      }
    });

    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(),
                                             MockStages.createPipelineConfigurationSourceProcessorTarget())
        .build(pipelineRunner);
    StagePipe pipe = (StagePipe) pipeline.getPipes()[8];
    BatchMakerImpl batchMaker = Mockito.mock(BatchMakerImpl.class);
    Mockito.when(batchMaker.getLanes()).thenReturn(ImmutableSet.of("t"));

    BatchImpl batch = Mockito.mock(BatchImpl.class);
    Mockito.when(batch.getSourceOffset()).thenReturn("offset2");

    PipeBatch pipeBatch = Mockito.mock(PipeBatch.class);
    Mockito.when(pipeBatch.startStage(Mockito.eq(pipe))).thenReturn(batchMaker);
    Mockito.when(pipeBatch.getBatch(Mockito.eq(pipe))).thenReturn(batch);

    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).startStage(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).getBatch(Mockito.eq(pipe));
    Mockito.verify(pipeBatch, Mockito.times(1)).completeStage(Mockito.eq(batchMaker));
    Mockito.verifyNoMoreInteractions(pipeBatch);
    Assert.assertTrue(write);
  }

}
