/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.CombinerPipe;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRunner;

import com.streamsets.datacollector.util.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCombinerPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testCombinerPipe() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new Pipeline.Builder(MockStages.createStageLibrary(), new Configuration(), "name", "myPipeline",
      "0", MockStages.createPipelineConfigurationSourceTarget()).build(pipelineRunner);
    CombinerPipe pipe = (CombinerPipe) pipeline.getPipes()[3];
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).combineLanes(Mockito.anyList(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

}
