/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MockStages {

  @SuppressWarnings("unchecked")
  public static StageConfiguration createSource(String instanceName, List<String> outputs) {

    return new StageConfiguration(instanceName, "default", "sourceName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(),
      outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createProcessor(String instanceName, List<String> inputs, List<String> outputs) {
    return new StageConfiguration(
      instanceName, "default", "processorName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, inputs, outputs);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration createTarget(String instanceName, List<String> inputs) {
    return new StageConfiguration(
      instanceName, "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, inputs, Collections.<String>emptyList());
  }

  private static Source sourceCapture;
  private static Processor processorCapture;
  private static Target targetCapture;
  private static Target errorCapture;

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

  // it must be called after the pipeline is built
  public static void setErrorStageCapture(Target t) {
    errorCapture = t;
  }

  public static class MSource implements Source {

    @Override
    public List<ConfigIssue> validateConfigs(Info info, Context context) throws StageException {
      if (sourceCapture != null) {
        return sourceCapture.validateConfigs(info, context);
      } else {
        return Collections.emptyList();
      }
    }

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
        ((OffsetCommitter) sourceCapture).commit(offset);
      }
    }
  }

  public static class MProcessor implements Processor {

    @Override
    public List<ConfigIssue> validateConfigs(Info info, Processor.Context context) throws StageException {
      if (processorCapture != null) {
        return processorCapture.validateConfigs(info, context);
      } else {
        return Collections.emptyList();
      }
    }

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
    public List<ConfigIssue> validateConfigs(Info info, Target.Context context) throws StageException {
      if (targetCapture != null) {
        return targetCapture.validateConfigs(info, context);
      } else {
        return Collections.emptyList();
      }
    }

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

  public static class ETarget implements Target {

    @Override
    public List<ConfigIssue> validateConfigs(Info info, Target.Context context) throws StageException {
      if (errorCapture != null) {
        return errorCapture.validateConfigs(info, context);
      } else {
        return Collections.emptyList();
      }
    }

    @Override
    public void init(Info info, Context context) throws StageException {
      if (errorCapture != null) {
        errorCapture.init(info, context);
      }
    }

    @Override
    public void destroy() {
      if (errorCapture != null) {
        errorCapture.destroy();
      }
    }

    @Override
    public void write(Batch batch) throws StageException {
      if (errorCapture != null) {
        errorCapture.write(batch);
      }
    }
  }
  public static StageLibraryTask createStageLibrary() {
    return new MockStageLibraryTask.Builder().build();
  }

  public static class MockStageLibraryTask implements StageLibraryTask {
    private final List<StageDefinition> stages;

    private MockStageLibraryTask(Collection<StageDefinition> stages) {
      this.stages = ImmutableList.copyOf(stages);
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
    public static class Builder {
      private final Map<String, StageDefinition> stages;

      public Builder() {
        StageDefinition sDef = new StageDefinition(
          MSource.class.getName(), "sourceName", "1.0.0", "sourceLabel",
          "sourceDesc", StageType.SOURCE, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null, false, 1, null
        );
        sDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

        StageDefinition socDef = new StageDefinition(
          MSourceOffsetCommitter.class.getName(), "sourceOffsetCommitterName", "1.0.0", "sourceOffsetCommitterLabel",
          "sourceDesc", StageType.SOURCE, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null, false, 1, null
        );
        socDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

        StageDefinition pDef = new StageDefinition(MProcessor.class.getName(), "processorName", "1.0.0", "sourcelabel",
          "sourceDescription", StageType.PROCESSOR, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null,
          false, 1, null);
        pDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

        StageDefinition tDef = new StageDefinition(
          MTarget.class.getName(), "targetName", "1.0.0", "targetLabel",
          "targetDesc", StageType.TARGET, false, true, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null, false, 0, null
        );
        tDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

        StageDefinition eDef = new StageDefinition(
          ETarget.class.getName(), "errorTarget", "1.0.0", "errorTarget",
          "Error Target", StageType.TARGET, true, false, true, Collections.<ConfigDefinition>emptyList(),
          null/*raw source definition*/, "", null, false, 0, null
        );
        eDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());

        ConfigDefinition depConfDef = new ConfigDefinition(
          "dependencyConfName", ConfigDef.Type.NUMBER, "dependencyConfLabel", "dependencyConfDesc", "", true,
          "groupName", "dependencyConfFieldName", null, "", null, 0, Collections.<ElFunctionDefinition>emptyList(),
          Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0, Collections.<String> emptyList());
        List<Object> triggeredBy = new ArrayList<>();
        triggeredBy.add(1);
        ConfigDefinition triggeredConfDef = new ConfigDefinition(
          "triggeredConfName", ConfigDef.Type.NUMBER, "triggeredConfLabel", "triggeredConfDesc", "", true,
          "groupName", "triggeredConfFieldName", null, "dependencyConfName", triggeredBy, 0,
          Collections.<ElFunctionDefinition>emptyList(), Collections.<ElConstantDefinition>emptyList(), Long.MIN_VALUE, Long.MAX_VALUE, "text/plain", 0,
          Collections.<String> emptyList());
        StageDefinition swcDef = new StageDefinition(
          MSource.class.getName(), "sourceWithConfigsName", "1.0.0", "sourceWithConfigsLabel",
          "sourceWithConfigsDesc", StageType.SOURCE, false, true, true, Lists.newArrayList(depConfDef, triggeredConfDef),
          null/*raw source definition*/, "", null, false, 1, null);
        swcDef.setLibrary("default", "", Thread.currentThread().getContextClassLoader());
        StageDefinition[] stageDefs = new StageDefinition[]{sDef, socDef, pDef, tDef, swcDef, eDef};
        stages = new HashMap<>();
        for (StageDefinition def : stageDefs) {
          if (stages.containsKey(def.getName())) {
            throw new IllegalStateException("Duplicate stage at " + def.getName());
          }
          stages.put(def.getName(), def);
        }
      }

      public Builder override(StageDefinition def) {
        if (stages.containsKey(def.getName())) {
          stages.put(def.getName(), def);
        } else {
          throw new IllegalStateException("Expected stage at " + def.getName());
        }
        return this;
      }
      public Builder overrideClass(String name, String className) {
        if (stages.containsKey(name)) {
          StageDefinition oldDef = stages.get(name);
          StageDefinition newDef = new StageDefinition(
            className, oldDef.getName(), oldDef.getVersion(), oldDef.getLabel(),
            oldDef.getDescription(), oldDef.getType(), oldDef.isErrorStage(), oldDef.hasRequiredFields(),
            oldDef.hasOnRecordError(), oldDef.getConfigDefinitions(),
            oldDef.getRawSourceDefinition(), oldDef.getIcon(), oldDef.getConfigGroupDefinition(),
            oldDef.isVariableOutputStreams(), oldDef.getOutputStreams(), oldDef.getOutputStreamLabelProviderClass()
          );
          newDef.setLibrary(oldDef.getLibrary(), oldDef.getLibraryLabel(), oldDef.getStageClassLoader());
          stages.put(name, newDef);
        } else {
          throw new IllegalStateException("Expected stage at " + name);
        }
        return this;
      }

      public StageLibraryTask build() {
        return new MockStageLibraryTask(stages.values());
      }
    }
  }

  public static void resetStageCaptures() {
    sourceCapture = null;
    processorCapture = null;
    targetCapture = null;
  }

  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget() {
    return createPipelineConfigurationSourceProcessorTarget(PipelineStoreTask.SCHEMA_VERSION);
  }

  @SuppressWarnings("unchecked")
  public static StageConfiguration getErrorStageConfig() {
    return new StageConfiguration("errorStage", "default", "errorTarget", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(),
      Collections.<String>emptyList());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceProcessorTarget(int schemaVersion) {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    stages.add(target);

    return new PipelineConfiguration(schemaVersion, UUID.randomUUID(), null, null, stages, getErrorStageConfig(),
      Collections.<String, Object> emptyMap());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceOffsetCommitterProcessorTarget() {
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceOffsetCommitterName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(), ImmutableList.of("s"));
    stages.add(source);
    StageConfiguration processor = new StageConfiguration("p", "default", "processorName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("s"), ImmutableList.of("p"));
    stages.add(processor);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, null, stages,
                                     getErrorStageConfig(), Collections.<String, Object> emptyMap());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineWithRequiredDependentConfig() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();

    StageConfiguration source = new StageConfiguration("s", "default", "sourceWithConfigsName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(), lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, null, stages,
                                     getErrorStageConfig(), Collections.<String, Object> emptyMap());
  }

  @SuppressWarnings("unchecked")
  public static PipelineConfiguration createPipelineConfigurationSourceTarget() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(),
      lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, null, stages,
                                     getErrorStageConfig(), Collections.<String, Object> emptyMap());
  }

  public static PipelineConfiguration createPipelineConfigurationSourceTwoTargets() {
    List<String> lanes = ImmutableList.of("a");
    List<StageConfiguration> stages = new ArrayList<>();
    StageConfiguration source = new StageConfiguration("s", "default", "sourceName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, Collections.<String>emptyList(), lanes);
    stages.add(source);
    StageConfiguration target = new StageConfiguration("t1", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    target = new StageConfiguration("t2", "default", "targetName", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, lanes, Collections.<String>emptyList());
    stages.add(target);
    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(), null, null, stages,
                                     getErrorStageConfig(), Collections.<String, Object> emptyMap());
  }

}
