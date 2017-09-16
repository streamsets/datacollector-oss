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
import com.streamsets.datacollector.main.RuntimeInfo;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;


public class TestMultiplexerPipe {

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexerPipeOneLaneWithEvents() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceTargetWithEventsProcessed())
      .build(pipelineRunner);
    MultiplexerPipe pipe = (MultiplexerPipe) pipeline.getRunners().get(0).get(1);
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(eq("t::o"), eq("t--t::s::m"));
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLane(eq("e::o"), eq("e--e::s::m"));
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexerPipeTwoLane() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceTwoTargets())
      .build(pipelineRunner);
    MultiplexerPipe pipe = (MultiplexerPipe) pipeline.getRunners().get(0).get(1);
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLaneCopying(Mockito.anyString(), Mockito.anyList());
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiplexerPipeTwoLaneWithTwoEventLanes() throws Exception {
    PipelineRunner pipelineRunner = Mockito.mock(PipelineRunner.class);
    Mockito.when(pipelineRunner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
        Mockito.when(pipelineRunner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Pipeline pipeline = new MockPipelineBuilder()
      .withPipelineConf(MockStages.createPipelineConfigurationSourceTwoTargetsTwoEvents())
      .build(pipelineRunner);
    MultiplexerPipe pipe = (MultiplexerPipe) pipeline.getRunners().get(0).get(1);
    PipeBatch pipeBatch = Mockito.mock(FullPipeBatch.class);
    pipe.process(pipeBatch);
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLaneCopying(eq("t::o"), (List<String>)argThat(contains("t--t1::s::m", "t--t2::s::m")));
    Mockito.verify(pipeBatch, Mockito.times(1)).moveLaneCopying(eq("e::o"), (List<String>)argThat(contains("e--t3::s::m", "e--t4::s::m")));
    Mockito.verifyNoMoreInteractions(pipeBatch);
  }

}
