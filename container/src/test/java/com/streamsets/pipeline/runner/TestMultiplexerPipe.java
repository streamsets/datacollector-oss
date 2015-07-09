/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.main.RuntimeInfo;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMultiplexerPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexerPipeOneLane() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name", "name", "0",
                                             MockStages.createPipelineConfigurationSourceTarget()).build(pipelineRunner);
    MultiplexerPipe pipe = (MultiplexerPipe) pipeline.getPipes()[2];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(Mockito.anyString(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexerPipeTwoLane() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), "name", "name", "0",
                                             MockStages.createPipelineConfigurationSourceTwoTargets()).build(pipelineRunner);
    MultiplexerPipe pipe = (MultiplexerPipe) pipeline.getPipes()[2];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLaneCopying(Mockito.anyString(), Mockito.anyList());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

}
