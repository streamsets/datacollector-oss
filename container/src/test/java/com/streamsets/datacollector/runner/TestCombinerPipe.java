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

import com.streamsets.datacollector.main.RuntimeInfo;

import org.junit.Test;
import org.mockito.Mockito;

public class TestCombinerPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testCombinerPipe() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder().build(pipelineRunner);
    CombinerPipe pipe = (CombinerPipe) pipeline.getRunners().get(0).get(2);
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).combineLanes(Mockito.anyList(), Mockito.anyString());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

}
