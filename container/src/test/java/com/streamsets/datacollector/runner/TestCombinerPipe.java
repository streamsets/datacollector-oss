/**
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

import com.streamsets.datacollector.lineage.LineagePublisherTask;
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
    Pipeline pipeline = new Pipeline.Builder(
      MockStages.createStageLibrary(),
      new Configuration(),
      "name",
      "myPipeline",
      "0",
      MockStages.userContext(),
      MockStages.createPipelineConfigurationSourceTarget(),
      Mockito.mock(LineagePublisherTask.class)
    ).build(pipelineRunner);
    CombinerPipe pipe = (CombinerPipe) pipeline.getRunners().get(0).get(2);
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).combineLanes(Mockito.anyList(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

}
