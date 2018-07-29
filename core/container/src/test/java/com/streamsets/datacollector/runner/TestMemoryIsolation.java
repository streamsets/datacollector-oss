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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.memory.MemoryUsageCollector;
import com.streamsets.datacollector.memory.TestMemoryUsageCollector;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This test ensures we are not traversing the entire object graph. This can happen
 * if we add a link from a stage to the rest of the object graph. For example
 * if we added a reference from a stage to StageRuntime we'd see this behavior.
 */
public class TestMemoryIsolation {
  @BeforeClass
  public static void setupClass() throws Exception {
    TestMemoryUsageCollector.initalizeMemoryUtility();
  }

  @Before
  public void setUp() {
    MockStages.resetStageCaptures();
  }

  private static Set<Stage> createStages(StageLibraryTask lib) throws Exception {
    List<StageConfiguration> stageDefs = ImmutableList.of(
      MockStages.createSource("s", ImmutableList.of("s")),
      MockStages.createProcessor("p", ImmutableList.of("s"), ImmutableList.of("p")),
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
        "label",
        null,
        pipelineConfigs,
        null,
        stageDefs,
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    Pipeline.Builder builder = new MockPipelineBuilder()
      .withStageLib(lib)
      .withPipelineConf(pipelineConf)
      .build();

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    Mockito.when(runner.getMetrics()).thenReturn(new MetricRegistry());
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Set<Stage> stages = new HashSet<>();
    Pipeline pipeline = builder.build(runner);
    pipeline.init(false);

    // Working with just one runner
    pipeline.getRunners().get(0).forEach(pipe -> stages.add(pipe.getStage().getStage()));

    return stages;
  }

  @Test
  public void testSourceWithLargeBuffer() throws Exception {
    StageLibraryTask lib = new MockStages.MockStageLibraryTask.Builder()
      .overrideClass("sourceName", SourceWithLargeBuffer.class).build();
    for (Stage stage : createStages(lib)) {
      if (stage instanceof SourceWithLargeBuffer) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be >= than 10MB, was: " +  memoryUsed,
          memoryUsed >= 1024 * 1024 * 10);
      } else if (stage instanceof Processor) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " +  memoryUsed,
          memoryUsed <= 1024);
      } else if (stage instanceof Target) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " + memoryUsed,
          memoryUsed <= 1024);
      } else {
        Assert.fail("Unknown stage type: " + stage);
      }
    }
  }

  @Test
  public void testProcessorWithLargeBuffer() throws Exception {
    StageLibraryTask lib = new MockStages.MockStageLibraryTask.Builder()
      .overrideClass("processorName", ProcessorWithLargeBuffer.class).build();
    for (Stage stage : createStages(lib)) {
      if (stage instanceof Source) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " +  memoryUsed,
          memoryUsed <= 1024);
      } else if (stage instanceof ProcessorWithLargeBuffer) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be >= than 10MB, was: " +  memoryUsed,
          memoryUsed >= 1024 * 1024 * 10);
      } else if (stage instanceof Target) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " + memoryUsed,
          memoryUsed <= 1024);
      } else {
        Assert.fail("Unknown stage type: " + stage);
      }
    }
  }

  @Test
  public void testTargetWithLargeBuffer() throws Exception {
    StageLibraryTask lib = new MockStages.MockStageLibraryTask.Builder()
      .overrideClass("targetName", TargetWithLargeBuffer.class).build();
    for (Stage stage : createStages(lib)) {
      if (stage instanceof Source) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " + memoryUsed,
          memoryUsed <= 1024);
      } else if (stage instanceof Processor) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be < than 1KiB, was: " +  memoryUsed,
          memoryUsed <= 1024);
      } else if (stage instanceof TargetWithLargeBuffer) {
        long memoryUsed = MemoryUsageCollector.getMemoryUsageOfForTests(stage);
        Assert.assertTrue("Expected memory used to be >= than 10MB, was: " +  memoryUsed,
          memoryUsed >= 1024 * 1024 * 10);
      } else {
        Assert.fail("Unknown stage type: " + stage);
      }
    }
  }
  public static class TargetWithLargeBuffer implements Target {
    private final byte[] buffer;
    public TargetWithLargeBuffer() {
      buffer = new byte[1024 * 1024 * 10];
    }

    @Override
    public void write(Batch batch) throws StageException {

    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }
  }
  public static class ProcessorWithLargeBuffer implements Processor {
    private final byte[] buffer;
    public ProcessorWithLargeBuffer() {
      buffer = new byte[1024 * 1024 * 10];
    }

    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {

    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }
  }
  public static class SourceWithLargeBuffer implements Source {
    private final byte[] buffer;
    public SourceWithLargeBuffer() {
      buffer = new byte[1024 * 1024 * 10];
    }
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }

    @Override
    public List<ConfigIssue> init(Info info, Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }
  }
}
