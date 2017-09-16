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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.pipeline.api.Stage;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class TestLaneResolver {

  public StageRuntime createMockSource(String instanceName, List<String> outputLanes) {
    return createMockSource(instanceName, outputLanes, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public StageRuntime createMockSource(String instanceName, List<String> outputLanes, List<String> eventLanes) {
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getInputLanes()).thenReturn(Collections.EMPTY_LIST);
    Mockito.when(stageConf.getOutputLanes()).thenReturn(outputLanes);
    Mockito.when(stageConf.getEventLanes()).thenReturn(eventLanes);
    Mockito.when(stageConf.getOutputAndEventLanes()).thenReturn(Lists.newArrayList(Iterables.concat(outputLanes, eventLanes)));

    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn(instanceName);

    StageRuntime runtime = Mockito.mock(StageRuntime.class);
    Mockito.when(runtime.getConfiguration()).thenReturn(stageConf);
    Mockito.when(runtime.getInfo()).thenReturn(stageInfo);
    return runtime;
  }

  public StageRuntime createMockTarget(String instanceName, List<String> inputLanes) {
    return createMockTarget(instanceName, inputLanes, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public StageRuntime createMockTarget(String instanceName, List<String> inputLanes, List<String> eventLanes) {
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getInputLanes()).thenReturn(inputLanes);
    Mockito.when(stageConf.getOutputLanes()).thenReturn(Collections.EMPTY_LIST);
    Mockito.when(stageConf.getEventLanes()).thenReturn(eventLanes);
    Mockito.when(stageConf.getOutputAndEventLanes()).thenReturn(eventLanes);

    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn(instanceName);

    StageRuntime runtime = Mockito.mock(StageRuntime.class);
    Mockito.when(runtime.getConfiguration()).thenReturn(stageConf);
    Mockito.when(runtime.getInfo()).thenReturn(stageInfo);
    return runtime;
  }

  public StageRuntime createMockProcessor(String instanceName, List<String> inputLanes, List<String> outputLanes) {
    return createMockProcessor(instanceName, inputLanes, outputLanes, Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public StageRuntime createMockProcessor(String instanceName, List<String> inputLanes, List<String> outputLanes, List<String> eventLanes) {
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getInputLanes()).thenReturn(inputLanes);
    Mockito.when(stageConf.getOutputLanes()).thenReturn(outputLanes);
    Mockito.when(stageConf.getEventLanes()).thenReturn(eventLanes);
    Mockito.when(stageConf.getOutputAndEventLanes()).thenReturn(Lists.newArrayList(Iterables.concat(outputLanes, eventLanes)));

    Stage.Info stageInfo = Mockito.mock(Stage.Info.class);
    Mockito.when(stageInfo.getInstanceName()).thenReturn(instanceName);

    StageRuntime runtime = Mockito.mock(StageRuntime.class);
    Mockito.when(runtime.getConfiguration()).thenReturn(stageConf);
    Mockito.when(runtime.getInfo()).thenReturn(stageInfo);
    return runtime;
  }

  // s1 -> t1
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline1() {
    String sourceInstance = "s1";
    String targetInstance = "t1";
    List<StageRuntime> stages = ImmutableList.of(
      createMockSource(sourceInstance, ImmutableList.of(sourceInstance)),
      createMockTarget(targetInstance, ImmutableList.of(sourceInstance))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));

    // source observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(sourceInstance, targetInstance, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(0));

    // target stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(sourceInstance, targetInstance, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(1)
    );

    // target observer
    Assert.assertEquals(0, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(0, resolver.getObserverOutputLanes(1).size());

    // target multiplexer
    Assert.assertEquals(0, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(0, resolver.getMultiplexerOutputLanes(1).size());
  }

  // single output processor
  // s1 -> p1 -> t1
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline2() {
    String sourceInstance = "s1";
    String processorInstance = "p1";
    String targetInstance = "t1";
    List<StageRuntime> stages = ImmutableList.of(
        createMockSource(sourceInstance, ImmutableList.of(sourceInstance)),
        createMockProcessor(processorInstance, ImmutableList.of(sourceInstance), ImmutableList.of(processorInstance)),
        createMockTarget(targetInstance, ImmutableList.of(processorInstance))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));

    // source observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(sourceInstance, processorInstance, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(0));

    // processor stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(
        LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(sourceInstance, processorInstance, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
        resolver.getStageInputLanes(1)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(1));

    // processor observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(1));

    // processor multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance, targetInstance, processorInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(1));

    // target stage
    Assert.assertEquals(1, resolver.getStageInputLanes(2).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(2).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance, targetInstance, processorInstance)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(2)
    );

    // target observer
    Assert.assertEquals(0, resolver.getObserverInputLanes(2).size());
    Assert.assertEquals(0, resolver.getObserverOutputLanes(2).size());

    // target multiplexer
    Assert.assertEquals(0, resolver.getMultiplexerInputLanes(2).size());
    Assert.assertEquals(0, resolver.getMultiplexerOutputLanes(2).size());
  }

  // single output processors
  // s1 -> p1 -> p2 -> t1
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline3() {
    String sourceInstance = "s1";
    String processorInstance1 = "p1";
    String processorInstance2 = "p2";
    String targetInstance = "t1";
    List<StageRuntime> stages = ImmutableList.of(
        createMockSource(sourceInstance, ImmutableList.of(sourceInstance)),
        createMockProcessor(processorInstance1, ImmutableList.of(sourceInstance), ImmutableList.of(processorInstance1)),
        createMockProcessor(processorInstance2, ImmutableList.of(processorInstance1), ImmutableList.of(processorInstance2)),
        createMockTarget(targetInstance, ImmutableList.of(processorInstance2))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));

    // source observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(0));

    // processor1 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(1)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(1));

    // processor1 observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(1));

    // processor1 multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance1, processorInstance2, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(1));

    // processor2 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(2).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(2).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance1, processorInstance2, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(2)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(2));

    // processor2 observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(2).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(2));

    // processor2 multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(2).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(2));

    // target stage
    Assert.assertEquals(1, resolver.getStageInputLanes(3).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(3).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(3));

    // target observer
    Assert.assertEquals(0, resolver.getObserverInputLanes(3).size());
    Assert.assertEquals(0, resolver.getObserverOutputLanes(3).size());

    // target multiplexer
    Assert.assertEquals(0, resolver.getMultiplexerInputLanes(3).size());
    Assert.assertEquals(0, resolver.getMultiplexerOutputLanes(3).size());

    // getMatchingOutputLanes
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("s1", "p1", "s1")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("s1", resolver.getMultiplexerOutputLanes(0)));

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p1", "p2", "p1")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p1", resolver.getMultiplexerOutputLanes(1)));

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p2", "t1", "p2")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p2", resolver.getMultiplexerOutputLanes(2)));

    // getMatchingOutputLanes, negative test
    Assert.assertEquals(Collections.EMPTY_LIST,
                        LaneResolver.getMatchingOutputLanes("p1", resolver.getMultiplexerOutputLanes(0)));
  }

  // single output processors
  // s1 -> p1 -> p2 -> t1
  //        |--------->|
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline4() {
    String sourceInstance = "s1";
    String processorInstance1 = "p1";
    String processorInstance2 = "p2";
    String targetInstance = "t1";
    List<StageRuntime> stages = ImmutableList.of(
        createMockSource(sourceInstance, ImmutableList.of(sourceInstance)),
        createMockProcessor(processorInstance1, ImmutableList.of(sourceInstance), ImmutableList.of(processorInstance1)),
        createMockProcessor(processorInstance2, ImmutableList.of(processorInstance1), ImmutableList.of(processorInstance2)),
        createMockTarget(targetInstance, ImmutableList.of(processorInstance1, processorInstance2))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));

    // source observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(0));

    // processor1 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(1)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(1));

    // processor1 observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(1));

    // processor1 multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(2, resolver.getMultiplexerOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance1, processorInstance2, processorInstance1),
                            LaneResolver.createLane(processorInstance1, targetInstance, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(1));

    // processor2 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(2).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(2).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance1, processorInstance2, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(2)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(2));

    // processor2 observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(2).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(2));

    // processor2 multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(2).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(2));

    // target stage
    Assert.assertEquals(2, resolver.getStageInputLanes(3).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(3).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance1, targetInstance, processorInstance1), LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(3)
    );

    // target observer
    Assert.assertEquals(0, resolver.getObserverInputLanes(3).size());
    Assert.assertEquals(0, resolver.getObserverOutputLanes(3).size());

    // target multiplexer
    Assert.assertEquals(0, resolver.getMultiplexerInputLanes(3).size());
    Assert.assertEquals(0, resolver.getMultiplexerOutputLanes(3).size());


    // getMatchingOutputLanes

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p1", "p2", "p1"),
                                                                   LaneResolver.createLane("p1", "t1", "p1")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p1", resolver.getMultiplexerOutputLanes(1)));

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p2", "t1", "p2")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p2", resolver.getMultiplexerOutputLanes(2)));

    // getMatchingOutputLanes, negative test
    Assert.assertEquals(Collections.EMPTY_LIST,
                        LaneResolver.getMatchingOutputLanes("p1", resolver.getMultiplexerOutputLanes(0)));
  }

  // multiple output processor (p1)
  // s1 -> p1 -> p2 -> t1
  //        |--------->|
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline5() {
    String sourceInstance = "s1";
    String processorInstance1 = "p1";
    String processorInstance1a = "p1a";
    String processorInstance1b = "p1b";
    String processorInstance2 = "p2";
    String targetInstance = "t1";
    List<StageRuntime> stages = ImmutableList.of(
        createMockSource(sourceInstance, ImmutableList.of(sourceInstance)),
        createMockProcessor(processorInstance1, ImmutableList.of(sourceInstance), ImmutableList.of(processorInstance1a, processorInstance1b)),
        createMockProcessor(processorInstance2, ImmutableList.of(processorInstance1a), ImmutableList.of(processorInstance2)),
        createMockTarget(targetInstance, ImmutableList.of(processorInstance1b, processorInstance2))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));

    // source observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(sourceInstance), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(0));

    // processor1 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(2, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(sourceInstance, processorInstance1, sourceInstance)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(1)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1a, processorInstance1b), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(1));

    // processor1 observer
    Assert.assertEquals(2, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(2, resolver.getObserverOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1a, processorInstance1b), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1a, processorInstance1b), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(1));

    // processor1 multiplexer
    Assert.assertEquals(2, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(2, resolver.getMultiplexerOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance1a, processorInstance1b), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance1a, processorInstance2, processorInstance1),
                            LaneResolver.createLane(processorInstance1b, targetInstance, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(1));

    // processor2 stage
    Assert.assertEquals(1, resolver.getStageInputLanes(2).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(2).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance1a, processorInstance2, processorInstance1)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(2)
    );
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(2));

    // processor2 observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(2).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(2));

    // processor2 multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(2).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(2).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(processorInstance2), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(2));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(2));

    // target stage
    Assert.assertEquals(2, resolver.getStageInputLanes(3).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(3).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane(processorInstance1b, targetInstance, processorInstance1), LaneResolver.createLane(processorInstance2, targetInstance, processorInstance2)), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(3)
    );

    // target observer
    Assert.assertEquals(0, resolver.getObserverInputLanes(3).size());
    Assert.assertEquals(0, resolver.getObserverOutputLanes(3).size());

    // target multiplexer
    Assert.assertEquals(0, resolver.getMultiplexerInputLanes(3).size());
    Assert.assertEquals(0, resolver.getMultiplexerOutputLanes(3).size());

    // getMatchingOutputLanes

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p1a", "p2", "p1")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p1a", resolver.getMultiplexerOutputLanes(1)));

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p1b", "t1", "p1")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p1b", resolver.getMultiplexerOutputLanes(1)));

    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("p2", "t1", "p2")),
                                                  LaneResolver.MULTIPLEXER_OUT),
                        LaneResolver.getMatchingOutputLanes("p2", resolver.getMultiplexerOutputLanes(2)));

    // getMatchingOutputLanes, negative test
    Assert.assertEquals(Collections.EMPTY_LIST,
                        LaneResolver.getMatchingOutputLanes("p1", resolver.getMultiplexerOutputLanes(0)));
  }

  // multiple output processor (p1)
  // s -> t    e
  // |    |-->|
  // |------->|
  @Test
  @SuppressWarnings("unchecked")
  public void testPipeline6() {
    List<StageRuntime> stages = ImmutableList.of(
      createMockSource("s", ImmutableList.of("s"), ImmutableList.of("se")),
      createMockTarget("t", ImmutableList.of("s"), ImmutableList.of("te")),
      createMockTarget("e", ImmutableList.of("se", "te"))
    );
    LaneResolver resolver = new LaneResolver(stages);

    // source stage
    Assert.assertEquals(0, resolver.getStageInputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageOutputLanes(0).size());
    Assert.assertEquals(1, resolver.getStageEventLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("s"), LaneResolver.STAGE_OUT),
                        resolver.getStageOutputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("se"), LaneResolver.STAGE_OUT),
      resolver.getStageEventLanes(0));

    // source observer
    Assert.assertEquals(2, resolver.getObserverInputLanes(0).size());
    Assert.assertEquals(2, resolver.getObserverOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("s", "se"), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("s", "se"), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(0));

    // source multiplexer
    Assert.assertEquals(2, resolver.getMultiplexerInputLanes(0).size());
    Assert.assertEquals(2, resolver.getMultiplexerOutputLanes(0).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("s", "se"), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(0));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
          LaneResolver.createLane("s", "t", "s"),
          LaneResolver.createLane("se", "e", "s")),
        LaneResolver.MULTIPLEXER_OUT),
      resolver.getMultiplexerOutputLanes(0));

    // target stage
    Assert.assertEquals(1, resolver.getStageInputLanes(1).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(1).size());
    Assert.assertEquals(1, resolver.getStageEventLanes(1).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("s", "t", "s")), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(1)
    );

    // target observer
    Assert.assertEquals(1, resolver.getObserverInputLanes(1).size());
    Assert.assertEquals(1, resolver.getObserverOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("te"), LaneResolver.STAGE_OUT),
                        resolver.getObserverInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("te"), LaneResolver.OBSERVER_OUT),
                        resolver.getObserverOutputLanes(1));

    // target multiplexer
    Assert.assertEquals(1, resolver.getMultiplexerInputLanes(1).size());
    Assert.assertEquals(1, resolver.getMultiplexerOutputLanes(1).size());
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of("te"), LaneResolver.OBSERVER_OUT),
                        resolver.getMultiplexerInputLanes(1));
    Assert.assertEquals(LaneResolver.getPostFixed(ImmutableList.of(
                            LaneResolver.createLane("te", "e", "t")), LaneResolver.MULTIPLEXER_OUT),
                        resolver.getMultiplexerOutputLanes(1));

    // event stage
    Assert.assertEquals(2, resolver.getStageInputLanes(2).size());
    Assert.assertEquals(0, resolver.getStageOutputLanes(2).size());
    Assert.assertEquals(0, resolver.getStageEventLanes(2).size());
    Assert.assertEquals(
      LaneResolver.getPostFixed(ImmutableList.of(LaneResolver.createLane("se", "e", "s"), LaneResolver.createLane("te", "e", "t")), LaneResolver.MULTIPLEXER_OUT),
      resolver.getStageInputLanes(2)
    );
  }
}
