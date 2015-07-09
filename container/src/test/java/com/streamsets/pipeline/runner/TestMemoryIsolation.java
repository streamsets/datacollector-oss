/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.memory.MemoryUsageCollector;
import com.streamsets.pipeline.memory.TestMemoryUsageCollector;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
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
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<>(2);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(),
      null, pipelineConfigs, null, stageDefs, MockStages.getErrorStageConfig());
    Pipeline.Builder builder = new Pipeline.Builder(lib, "name",  "name", "0", pipelineConf);

    PipelineRunner runner = Mockito.mock(PipelineRunner.class);
    MetricRegistry metrics = Mockito.mock(MetricRegistry.class);
    Mockito.when(runner.getMetrics()).thenReturn(metrics);
    Mockito.when(runner.getRuntimeInfo()).thenReturn(Mockito.mock(RuntimeInfo.class));

    Set<Stage> stages = new HashSet<>();
    Pipeline pipeline = builder.build(runner);
    pipeline.init();
    for (Pipe pipe : pipeline.getPipes()) {
      stages.add(pipe.getStage().getStage());
    }
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
    public List<ConfigIssue> validateConfigs(Info info, Context context) throws StageException {
      return null;
    }

    @Override
    public void init(Info info, Context context) throws StageException {

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
    public List<ConfigIssue> validateConfigs(Info info, Context context) throws StageException {
      return null;
    }

    @Override
    public void init(Info info, Context context) throws StageException {

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
    public List<ConfigIssue> validateConfigs(Info info, Context context) throws StageException {
      return null;
    }

    @Override
    public void init(Info info, Context context) throws StageException {

    }

    @Override
    public void destroy() {

    }

    @Override
    public int getParallelism() {
      return 1;
    }
  }
}
