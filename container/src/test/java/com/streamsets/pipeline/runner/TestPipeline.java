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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestPipeline {

  @Test
  public void testBuilder() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(UUID.randomUUID(),
      pipelineConfigs, null, stageDefs);
    Pipeline.Builder builder = new Pipeline.Builder(lib, "name", pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);

    Pipeline pipeline = builder.build(runner);

    // assert the pipes
    Pipe[] pipes = pipeline.getPipes();
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
    List<String> prevOutput = new ArrayList<String>();
    for (Pipe pipe : pipes) {
      Assert.assertEquals(prevOutput, pipe.getInputLanes());
      prevOutput = pipe.getOutputLanes();
    }
    Assert.assertTrue(prevOutput.isEmpty());

    List<Stage.Info> infos = new ArrayList<Stage.Info>();
    infos.add(null);
    infos.add(null);
    infos.add(null);

    // assert Stage.Info
    for (int i =0; i < pipes.length; i++) {
      StageRuntime stage = pipes[i].getStage();
      Assert.assertNotNull(stage.getInfo());
      String instanceName;
      String stageName;
      String stageVersion;
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
        Assert.assertEquals(ImmutableSet.of("s"), (((Source.Context)stage.getContext()).getOutputLanes()));
      } else if (i < 7) {
        Assert.assertEquals(ImmutableSet.of("p"), (((Processor.Context)stage.getContext()).getOutputLanes()));
      }
    }
  }

  @Test
  public void testContextRecordCreation() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(UUID.randomUUID(),
       pipelineConfigs, null,
      stageDefs);
    Pipeline.Builder builder = new Pipeline.Builder(lib, "name", pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);

    Pipeline pipeline = builder.build(runner);

    pipeline.configure(new Configuration());
    
    // assert the pipes
    Pipe[] pipes = pipeline.getPipes();

    Source.Context sourceContext = pipes[0].getStage().getContext();
    Processor.Context processorContext = pipes[3].getStage().getContext();

    Record record = sourceContext.createRecord("a");
    Assert.assertEquals("a", record.getHeader().getSourceId());
    Assert.assertEquals("s", record.getHeader().getStageCreator());
    Assert.assertNull(record.getHeader().getRaw());
    Assert.assertNull(record.getHeader().getRawMimeType());

    record = processorContext.createRecord(record, new byte[0], "mime");
    Assert.assertEquals("a", record.getHeader().getSourceId());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertEquals("mime", record.getHeader().getRawMimeType());

    record = processorContext.cloneRecord(record);
    Assert.assertEquals("a", record.getHeader().getSourceId());
    Assert.assertEquals("p", record.getHeader().getStageCreator());
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertEquals("mime", record.getHeader().getRawMimeType());
  }

  @Test
  public void testLifecycleDelegation() throws Exception {
    StageLibraryTask lib = MockStages.createStageLibrary();
    List<StageConfiguration> stageDefs = ImmutableList.of(
        MockStages.createSource("s", ImmutableList.of("s")),
        MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
        MockStages.createTarget("t", ImmutableList.of("p"))
    );
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(UUID.randomUUID(),
       pipelineConfigs, null, stageDefs);
    Pipeline.Builder builder = new Pipeline.Builder(lib, "name", pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);

    Source source = Mockito.mock(Source.class);
    Processor processor = Mockito.mock(Processor.class);
    Target target = Mockito.mock(Target.class);
    MockStages.setSourceCapture(source);
    MockStages.setProcessorCapture(processor);
    MockStages.setTargetCapture(target);

    Observer observer = Mockito.mock(Observer.class);
    builder.setObserver(observer);

    Pipeline pipeline = builder.build(runner);

    Configuration conf = new Configuration();
    pipeline.configure(conf);
    Mockito.verify(observer, Mockito.times(1)).configure(conf);

    Mockito.verifyZeroInteractions(source);
    Mockito.verifyZeroInteractions(processor);
    Mockito.verifyZeroInteractions(target);
    pipeline.init();
    Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Source.Context.class));
    Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                     Mockito.any(Processor.Context.class));
    Mockito.verify(target, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
    Mockito.verifyNoMoreInteractions(source);
    Mockito.verifyNoMoreInteractions(processor);
    Mockito.verifyNoMoreInteractions(target);

    Mockito.reset(runner);
    pipeline.run();
    Mockito.verify(runner, Mockito.times(1)).run(pipeline.getPipes());
    Mockito.verifyNoMoreInteractions(runner);

    pipeline.destroy();
    Mockito.verify(source, Mockito.times(1)).destroy();
    Mockito.verify(processor, Mockito.times(1)).destroy();
    Mockito.verify(target, Mockito.times(1)).destroy();
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
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(UUID.randomUUID(),
       pipelineConfigs, null, stageDefs);
    Pipeline.Builder builder = new Pipeline.Builder(lib, "name", pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);

    Source source = Mockito.mock(Source.class);
    Processor processor = Mockito.mock(Processor.class);
    ErrorId id = new ErrorId() {
      @Override
      public String getMessage() {
        return "";
      }
    };

    // test checked exception on init

    Mockito.doThrow(new StageException(id)).when(processor).init(Mockito.any(Stage.Info.class),
                                                                 Mockito.any(Processor.Context.class));
    Target target = Mockito.mock(Target.class);
    MockStages.setSourceCapture(source);
    MockStages.setProcessorCapture(processor);
    MockStages.setTargetCapture(target);

    Pipeline pipeline = builder.build(runner);

    try {
      pipeline.init();
      Assert.fail();
    } catch (StageException ex) {
      Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Source.Context.class));
      Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                       Mockito.any(Processor.Context.class));
      Mockito.verify(target, Mockito.times(0)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
      Mockito.verify(source, Mockito.times(1)).destroy();
      Mockito.verifyNoMoreInteractions(processor);
      Mockito.verifyNoMoreInteractions(target);
    }

    // test runtime exception on init

    Mockito.reset(source);
    Mockito.reset(processor);
    Mockito.reset(target);
    Mockito.doThrow(new RuntimeException()).when(processor).init(Mockito.any(Stage.Info.class),
                                                                 Mockito.any(Processor.Context.class));

    pipeline = builder.build(runner);

    try {
      pipeline.init();
      Assert.fail();
    } catch (RuntimeException ex) {
      Mockito.verify(source, Mockito.times(1)).init(Mockito.any(Stage.Info.class), Mockito.any(Source.Context.class));
      Mockito.verify(processor, Mockito.times(1)).init(Mockito.any(Stage.Info.class),
                                                       Mockito.any(Processor.Context.class));
      Mockito.verify(target, Mockito.times(0)).init(Mockito.any(Stage.Info.class), Mockito.any(Target.Context.class));
      Mockito.verify(source, Mockito.times(1)).destroy();
      Mockito.verifyNoMoreInteractions(processor);
      Mockito.verifyNoMoreInteractions(target);
    }

    // test exception on destroy

    Mockito.reset(source);
    Mockito.reset(processor);
    Mockito.reset(target);
    Mockito.doThrow(new RuntimeException()).when(processor).destroy();

    pipeline = builder.build(runner);

    pipeline.init();
    pipeline.destroy();
    Mockito.verify(source, Mockito.times(1)).destroy();
    Mockito.verify(processor, Mockito.times(1)).destroy();
    Mockito.verify(target, Mockito.times(1)).destroy();

  }

}
