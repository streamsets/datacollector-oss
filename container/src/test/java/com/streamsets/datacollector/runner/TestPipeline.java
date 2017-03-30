/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipeline {

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
  }

  private Pipe[] getSourceAndPipelinePipes(Pipeline pipeline) {
    List<Pipe> p = new ArrayList<>(1 + pipeline.getRunners().get(0).size());
    p.add(pipeline.getSourcePipe());
    p.addAll(pipeline.getRunners().get(0).getPipes());
    return p.toArray(new Pipe[p.size()]);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuilder() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION, "pipelineId", UUID.randomUUID(),
        "label", null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig(), MockStages.getStatsAggregatorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Pipeline pipeline = builder.build(runner);

    // assert the pipes
    Pipe[] pipes = getSourceAndPipelinePipes(pipeline);
    Assert.assertEquals(9, pipes.length);
    Assert.assertTrue(pipes[0] instanceof StagePipe);
    Assert.assertTrue(pipes[1] instanceof ObserverPipe);
    Assert.assertTrue(pipes[2] instanceof MultiplexerPipe);
    Assert.assertTrue(pipes[3] instanceof CombinerPipe);
    Assert.assertTrue(pipes[4] instanceof StagePipe);
    Assert.assertTrue(pipes[5] instanceof ObserverPipe);
    Assert.assertTrue(pipes[6] instanceof MultiplexerPipe);
    Assert.assertTrue(pipes[7] instanceof CombinerPipe);
    Assert.assertTrue(pipes[8] instanceof StagePipe);

    // assert the pipes wiring (in this case is just lineal, the TestLaneResolver covers the diff scenarios)
    List<String> prevOutput = new ArrayList<>();
    for (Pipe pipe : pipes) {
      Assert.assertEquals(prevOutput, pipe.getInputLanes());
      prevOutput = pipe.getOutputLanes();
    }
    Assert.assertTrue(prevOutput.isEmpty());

    List<Stage.Info> infos = new ArrayList<>();
    infos.add(null);
    infos.add(null);
    infos.add(null);

    // assert Stage.Info
    for (int i =0; i < pipes.length; i++) {
      StageRuntime stage = pipes[i].getStage();
      Assert.assertNotNull(stage.getInfo());
      String instanceName;
      String stageName;
      int stageVersion;
      if (i < 3) {
        instanceName = "s";
        stageName = stageDefs.get(0).getStageName();
        stageVersion = stageDefs.get(0).getStageVersion();
        infos.set(0, stage.getInfo());
      } else if (i < 7) {
        instanceName = "p";
        stageName = stageDefs.get(1).getStageName();
        stageVersion = stageDefs.get(1).getStageVersion();
        infos.set(1, stage.getInfo());
      } else {
        instanceName = "t";
        stageName = stageDefs.get(2).getStageName();
        stageVersion = stageDefs.get(2).getStageVersion();
        infos.set(2, stage.getInfo());
      }
      Assert.assertEquals(instanceName, stage.getInfo().getInstanceName());
      Assert.assertEquals(stageName, stage.getInfo().getName());
      Assert.assertEquals(stageVersion, stage.getInfo().getVersion());
    }

    // assert stage context
    for (int i =0; i < pipes.length; i++) {
      StageRuntime stage = pipes[i].getStage();
      Assert.assertNotNull(stage.getContext());
      Assert.assertSame(metrics, stage.getContext().getMetrics());
      Assert.assertEquals(infos, stage.getContext().getPipelineInfo());
      if (i < 3) {
        Assert.assertEquals(ImmutableList.of("s"), (((Source.Context)stage.getContext()).getOutputLanes()));
      } else if (i < 7) {
        Assert.assertEquals(ImmutableList.of("p"), (((Processor.Context)stage.getContext()).getOutputLanes()));
      }
    }
  }

  @Test
  public void testPipesWithEvents() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s"), ImmutableList.of("se")),          // Source with normal and event output
        MockStages.createTarget("t", ImmutableList.of("s"), ImmutableList.of("te")),          // Target with event output
        MockStages.createExecutor("e", ImmutableList.of("se", "te"), Collections.<String>emptyList()) // Even target
    );

    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION, "pipelineId", UUID.randomUUID(),"label",
        null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig(), MockStages.getStatsAggregatorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Pipeline pipeline = builder.build(runner);

    // assert the pipes
    Pipe[] pipes = getSourceAndPipelinePipes(pipeline);
    Assert.assertEquals(9, pipes.length);
    Assert.assertTrue(pipes[0] instanceof StagePipe);
    Assert.assertEquals("s", pipes[0].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[1] instanceof ObserverPipe);
    Assert.assertEquals("s", pipes[1].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[2] instanceof MultiplexerPipe);
    Assert.assertEquals("s", pipes[2].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[3] instanceof CombinerPipe);
    Assert.assertEquals("t", pipes[3].getStage().getConfiguration().getInstanceName());

    Assert.assertTrue(pipes[4] instanceof StagePipe);
    Assert.assertEquals("t", pipes[4].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[5] instanceof ObserverPipe);
    Assert.assertEquals("t", pipes[5].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[6] instanceof MultiplexerPipe);
    Assert.assertEquals("t", pipes[6].getStage().getConfiguration().getInstanceName());

    Assert.assertTrue(pipes[7] instanceof CombinerPipe);
    Assert.assertEquals("e", pipes[7].getStage().getConfiguration().getInstanceName());
    Assert.assertTrue(pipes[8] instanceof StagePipe);
    Assert.assertEquals("e", pipes[8].getStage().getConfiguration().getInstanceName());
  }

  @Test
  public void testContextRecordCreation() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
       PipelineConfigBean.VERSION, "pipelineId", UUID.randomUUID(),
        "label", null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig(), MockStages.getStatsAggregatorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Pipeline pipeline = builder.build(runner);

    // assert the pipes
    Pipe[] pipes = getSourceAndPipelinePipes(pipeline);

    Source.Context sourceContext = pipes[0].getStage().getContext();
    Processor.Context processorContext = pipes[3].getStage().getContext();

    Record record = sourceContext.createRecord("a");
    Assert.assertEquals("a", record.getHeader().getSourceId());
    Assert.assertEquals("s", record.getHeader().getStageCreator());
    Assert.assertNull(record.getHeader().getRaw());
    Assert.assertNull(record.getHeader().getRawMimeType());

    record = processorContext.createRecord(record, "s1");
    Assert.assertEquals("a_s1", record.getHeader().getSourceId());
    Assert.assertEquals("", record.getHeader().getStagesPath());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertNull(record.getHeader().getRaw());
    Assert.assertNull(record.getHeader().getRawMimeType());

    record = processorContext.createRecord(record, new byte[0], "mode");
    Assert.assertEquals("a_s1", record.getHeader().getSourceId());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertEquals("mode", record.getHeader().getRawMimeType());

    record = processorContext.cloneRecord(record);
    Assert.assertEquals("a_s1", record.getHeader().getSourceId());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertEquals("mode", record.getHeader().getRawMimeType());

    record = processorContext.cloneRecord(record, "s2");
    Assert.assertEquals("a_s1_s2", record.getHeader().getSourceId());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertEquals("mode", record.getHeader().getRawMimeType());
  }

  @Test
  public void testLifecycleDelegation() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
       PipelineConfigBean.VERSION, "pipelineId", UUID.randomUUID(),
        "label", null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig(), MockStages.getStatsAggregatorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Source source = Mockito.mock(Source.class);
    Processor processor = Mockito.mock(Processor.class);
    Target target = Mockito.mock(Target.class);
    MockStages.setSourceCapture(source);
    MockStages.setProcessorCapture(processor);
    MockStages.setTargetCapture(target);

    Observer observer = Mockito.mock(Observer.class);
    builder.setObserver(observer);

    Pipeline pipeline = builder.build(runner);

    Mockito.verifyZeroInteractions(source);
    Mockito.verifyZeroInteractions(processor);
    Mockito.verifyZeroInteractions(target);
    pipeline.init();
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Source.Context.class));
    Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                     Mockito.any(Processor.Context.class));
    Mockito.verify(target, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Target.Context.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(target);

    Mockito.reset(runner);
    pipeline.run();
    //FIXME<Hari> investigate
    // Mockito.verifyNoMoreInteractions(runner);

    pipeline.destroy();
    Mockito.verify(runner, Mockito.times(1)).destroy(Mockito.any(SourcePipe.class), Mockito.any(List.class), Mockito.any(BadRecordsHandler.class), Mockito.any(StatsAggregationHandler.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(target);

  }

  @Test
  public void testInitDestroyException() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
       PipelineConfigBean.VERSION, "pipelineId", UUID.randomUUID(),
        "label", null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig(), MockStages.getStatsAggregatorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Source source = Mockito.mock(Source.class);
    Processor processor = Mockito.mock(Processor.class);

    // test checked exception on init

    Mockito.doThrow(new RuntimeException()).when(processor).init(Mockito.any(Stage.Info.class),
                                                                 Mockito.any(Processor.Context.class));
    Target target = Mockito.mock(Target.class);
    MockStages.setSourceCapture(source);
    MockStages.setProcessorCapture(processor);
    MockStages.setTargetCapture(target);

    Pipeline pipeline = builder.build(runner);

    Assert.assertFalse(pipeline.init().isEmpty());
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Source.Context.class));
    Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                     Mockito.any(Processor.Context.class));
    Mockito.verify(target, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Target.Context.class));
    pipeline.destroy();
    Mockito.verify(runner, Mockito.times(1)).destroy(Mockito.any(SourcePipe.class), Mockito.any(List.class), Mockito.any(BadRecordsHandler.class), Mockito.any(StatsAggregationHandler.class));

    // test runtime exception on init

    Mockito.reset(source);
    Mockito.reset(processor);
    Mockito.reset(target);
    Mockito.reset(runner);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Mockito.doThrow(new RuntimeException()).when(processor).init(Mockito.any(Stage.Info.class),
                                                                 Mockito.any(Processor.Context.class));

    pipeline = builder.build(runner);

    Assert.assertFalse(pipeline.init().isEmpty());
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Source.Context.class));
    Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                     Mockito.any(Processor.Context.class));
    Mockito.verify(target, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                  Mockito.any(Target.Context.class));

    pipeline.destroy();
    Mockito.verify(runner, Mockito.times(1)).destroy(Mockito.any(SourcePipe.class), Mockito.any(List.class), Mockito.any(BadRecordsHandler.class), Mockito.any(StatsAggregationHandler.class));

    // test exception on destroy

    Mockito.reset(source);
    Mockito.reset(processor);
    Mockito.reset(target);
    Mockito.reset(runner);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));
    Mockito.doThrow(new RuntimeException()).when(processor).destroy();

    pipeline = builder.build(runner);

    pipeline.init();
    pipeline.destroy();
    Mockito.verify(runner, Mockito.times(1)).destroy(Mockito.any(SourcePipe.class), Mockito.any(List.class), Mockito.any(BadRecordsHandler.class), Mockito.any(StatsAggregationHandler.class));
  }

  @Test
  public void testCreateAdditionalRunnersOnInit() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createPushSource("s", ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
      PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION,
        "pipelineId",
      UUID.randomUUID(),
      null,
      "",
      pipelineConfigs,
      null,
      stageDefs,
      MockStages.getErrorStageConfig(),
      MockStages.getStatsAggregatorStageConfig()
    );
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    // We want 10 pipeline runners to be created
    PushSource source = Mockito.mock(PushSource.class);
    Mockito.when(source.getNumberOfThreads()).thenReturn(10);

    Target target = Mockito.mock(Target.class);
    MockStages.setPushSourceCapture(source);
    MockStages.setTargetCapture(target);

    Pipeline pipeline = builder.build(runner);

    Assert.assertTrue(pipeline.init().isEmpty());

    // Origin is initialized only once
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(PushSource.Context.class));

    // But the Target is initialized 10 times (~10 different "virtual" instances)
    Mockito.verify(target, Mockito.times(10)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
    Assert.assertEquals(10, pipeline.getRunners().size());
  }

  @Test
  public void testCreateAdditionalRunnersOnInitLimitedByPipelineConfiguration() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createPushSource("s", ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<Config> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new Config("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new Config("stopPipelineOnError", false));
    pipelineConfigs.add(new Config("maxRunners", 5));
    pipelineConfigs.add(new Config("executionMode", ExecutionMode.STANDALONE));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
      PipelineStoreTask.SCHEMA_VERSION,
      PipelineConfigBean.VERSION,
        "pipelineId",
      UUID.randomUUID(),
      null,
      "",
      pipelineConfigs,
      null,
      stageDefs,
      MockStages.getErrorStageConfig(),
      MockStages.getStatsAggregatorStageConfig()
    );
    Pipeline.Builder builder = new Pipeline.Builder(lib, new Configuration(), "name", "name", "0", MockStages.userContext(), pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    // Origin reports 10 threads
    PushSource source = Mockito.mock(PushSource.class);
    Mockito.when(source.getNumberOfThreads()).thenReturn(10);

    Target target = Mockito.mock(Target.class);
    MockStages.setPushSourceCapture(source);
    MockStages.setTargetCapture(target);

    Pipeline pipeline = builder.build(runner);

    Assert.assertTrue(pipeline.init().isEmpty());

    // Origin is initialized only once
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(PushSource.Context.class));

    // But the Target is initialized 10 times (~10 different "virtual" instances)
    Mockito.verify(target, Mockito.times(5)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
    Assert.assertEquals(5, pipeline.getRunners().size());
  }

}
