/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class MockStages {

  public static StageLibraryTask createStageLibrary() {
    return new MockStageLibraryTask();
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createSource(String instanceName, List<String> outputs) {

    return new StageConfiguration(instanceName, "default", "sourceName", "1.0.0",
                                  Collections.EMPTY_LIST, null, Collections.EMPTY_LIST, outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createProcessor(String instanceName, List<String> inputs, List<String> outputs) {
    return new StageConfiguration(
      instanceName, "default", "processorName",  "1.0.0",
      Collections.EMPTY_LIST, null, inputs, outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createTarget(String instanceName, List<String> inputs) {
    return new StageConfiguration(
      instanceName, "default", "targetName",  "1.0.0",
      Collections.EMPTY_LIST, null, inputs, Collections.EMPTY_LIST);
  }

  private static Source sourceCapture;
  private static Processor processorCapture;
  private static Target targetCapture;

  // it must be called after the pipeline is built
  public static void setSourceCapture(Source s) {
    sourceCapture = s;
  }

  // it must be called after the pipeline is built
  public static void setProcessorCapture(Processor p) {
    processorCapture = p;
  }

  // it must be called after the pipeline is built
  public static void setTargetCapture(Target t) {
    targetCapture = t;
  }

  private static class MockStageLibraryTask implements StageLibraryTask {

    public static class MSource implements Source {
      @Override
      public void init(Info info, Context context) throws StageException {
        if (sourceCapture != null) {
          sourceCapture.init(info, context);
        }
      }

      @Override
      public void destroy() {
        if (sourceCapture != null) {
          sourceCapture.destroy();
        }
      }

      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        if (sourceCapture != null) {
          return sourceCapture.produce(lastSourceOffset, -1, batchMaker);
        }
        return null;
      }
    }

    public static class MSourceOffsetCommitter extends MSource implements OffsetCommitter {

      @Override
      public void commit(String offset) throws StageException {
        if (sourceCapture != null && sourceCapture instanceof OffsetCommitter) {
          ((OffsetCommitter)sourceCapture).commit(offset);
        }
      }
    }

    public static class MProcessor implements Processor {

      @Override
      public void init(Info info, Context context) throws StageException {
        if (processorCapture != null) {
          processorCapture.init(info, context);
        }
      }

      @Override
      public void destroy() {
        if (processorCapture != null) {
          processorCapture.destroy();
        }
      }

      @Override
      public void process(Batch batch, BatchMaker batchMaker) throws StageException {
        if (processorCapture != null) {
          processorCapture.process(batch, batchMaker);
        }
      }
    }

    public static class MTarget implements Target {
      @Override
      public void init(Info info, Context context) throws StageException {
        if (targetCapture != null) {
          targetCapture.init(info, context);
        }
      }

      @Override
      public void destroy() {
        if (targetCapture != null) {
          targetCapture.destroy();
        }
      }

      @Override
      public void write(Batch batch) throws StageException {
        if (targetCapture != null) {
          targetCapture.write(batch);
        }
      }
    }

    private List<StageDefinition> stages;

    @SuppressWarnings("unchecked")
    public MockStageLibraryTask() {
      stages = new ArrayList<>();
      StageDefinition sDef = new StageDefinition(
        MSource.class.getName(), "sourceName", "1.0.0", "sourceLabel",
        "sourceDesc", StageType.SOURCE, Collections.EMPTY_LIST, null/*raw source definition*/,"", null);
      sDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

      StageDefinition socDef = new StageDefinition(
          MSourceOffsetCommitter.class.getName(), "sourceOffsetCommitterName", "1.0.0", "sourceOffsetCommitterLabel",
          "sourceDesc", StageType.SOURCE, Collections.EMPTY_LIST, null/*raw source definition*/,"", null);
      socDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

      StageDefinition pDef = new StageDefinition(MProcessor.class.getName(), "processorName", "1.0.0", "sourcelabel",
          "sourceDescription", StageType.PROCESSOR, Collections.EMPTY_LIST, null/*raw source definition*/, "", null);

      pDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());
      StageDefinition tDef = new StageDefinition(
        MTarget.class.getName(), "targetName", "1.0.0", "targetLabel",
        "targetDesc", StageType.TARGET, Collections.EMPTY_LIST, null/*raw source definition*/, "", null);
      tDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());
      stages = ImmutableList.of(sDef, socDef, pDef, tDef);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public void init() {

    }

    @Override
    public void run() {

    }

    @Override
    public void waitWhileRunning() throws InterruptedException {

    }

    @Override
    public void stop() {

    }

    @Override
    public Status getStatus() {
      return null;
    }

    @Override
    public List<StageDefinition> getStages() {
      return stages;
    }

    @Override
    public StageDefinition getStage(String library, String name, String version) {
      for (StageDefinition def : stages) {
        if (def.getLibrary().equals(library) && def.getName().equals(name) && def.getVersion().equals(version)) {
          return def;
        }
      }
      return null;
    }

  }

  public static void resetStageCaptures() {
    sourceCapture = null;
    processorCapture = null;
    targetCapture = null;
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget() {
    List<StageConfiguration> stages = new ArrayList<StageConfiguration>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, Collections.EMPTY_LIST,
                                                       ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", "1.0.0",
                                                          Collections.EMPTY_LIST, null, ImmutableList.of("s"),
                                                          ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, ImmutableList.of("p"),
                                                       Collections.EMPTY_LIST);
    stages.add(target);
    return new PipelineConfiguration(UUID.randomUUID(), null, null, stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceOffsetCommitterProcessorTarget() {
    List<StageConfiguration> stages = new ArrayList<StageConfiguration>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceOffsetCommitterName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, Collections.EMPTY_LIST,
                                                       ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", "1.0.0",
                                                          Collections.EMPTY_LIST, null, ImmutableList.of("s"),
                                                          ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, ImmutableList.of("p"),
                                                       Collections.EMPTY_LIST);
    stages.add(target);
    return new PipelineConfiguration(UUID.randomUUID(), null, null, stages);
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTarget() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<StageConfiguration>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, Collections.EMPTY_LIST, lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
                                                      Collections.EMPTY_LIST, null, lanes, Collections.EMPTY_LIST);
    stages.add(target);
    return new PipelineConfiguration(UUID.randomUUID(), null, null, stages);
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTwoTargets() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<StageConfiguration>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
                                                       Collections.EMPTY_LIST, null, Collections.EMPTY_LIST, lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t1", "default", "targetName", "1.0.0",
                                                      Collections.EMPTY_LIST, null, lanes, Collections.EMPTY_LIST);
    stages.add(target);
    target = new StageConfiguration("t2", "default", "targetName", "1.0.0",
                                                      Collections.EMPTY_LIST, null, lanes, Collections.EMPTY_LIST);
    stages.add(target);
    return new PipelineConfiguration(UUID.randomUUID(), null, null, stages);
  }

}
