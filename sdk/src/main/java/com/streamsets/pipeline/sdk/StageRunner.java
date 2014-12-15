/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.container.ErrorMessage;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.sdk.annotationsprocessor.StageHelper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class StageRunner<S extends Stage> {

  enum Status { CREATED, INITIALIZED, DESTROYED}

  private final S stage;
  private final Stage.Info info;
  private final StageContext context;
  private Status status;

  private static Stage getStage(Class<? extends Stage> klass) {
    try {
      return klass.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private StageDef getStageDefinition(Class<? extends Stage> klass) {
    return klass.getAnnotation(StageDef.class);
  }

  private String getName(Class<? extends Stage> klass) {
    return StageHelper.getStageNameFromClassName(klass.getName());
  }

  private String getVersion(Class<? extends Stage> klass) {
    StageDef def = getStageDefinition(klass);
    return (def != null) ? def.version() : "<UNANNOTATED-STAGE>";
  }

  private Set<String> getStageConfigurationFields(Class<? extends Stage> klass) throws Exception {
    Set<String> names = new HashSet<>();
    for (Field field : klass.getFields()) {
      if (field.isAnnotationPresent(ConfigDef.class)) {
        names.add(field.getName());
      }
    }
    return names;
  }

  private void configureStage(S stage, Map<String, Object> configuration) {
    try {
      Set<String> fields = getStageConfigurationFields(stage.getClass());
      Set<String> configs = configuration.keySet();
      if (!fields.equals(configs)) {
        Set<String> missingConfigs = Sets.difference(fields, configs);
        Set<String> extraConfigs = Sets.difference(configs, fields);
        if (missingConfigs.size() + extraConfigs.size() > 0) {
          throw new RuntimeException(Utils.format(
              "Invalid stage configuration for '{}', Missing configurations '{}' and invalid configurations '{}'",
              stage.getClass().getName(), missingConfigs, extraConfigs));
        }
      }
      for (Field field : stage.getClass().getFields()) {
        if (field.isAnnotationPresent(ConfigDef.class)) {
          field.set(stage, configuration.get(field.getName()));
        }
      }
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  StageRunner(Class<S> stageClass, Map<String, Object> configuration, Set<String> outputLanes) {
    this((S) getStage(Utils.checkNotNull(stageClass, "stageClass")), configuration, outputLanes);
  }

  StageRunner(S stage, Map < String, Object > configuration, Set < String > outputLanes) {
    Utils.checkNotNull(stage, "stage");
    Utils.checkNotNull(configuration, "configuration");
    Utils.checkNotNull(outputLanes, "outputLanes");
    this.stage = stage;
    try {
      configureStage(stage, configuration);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String name = getName(stage.getClass());
    String version = getVersion(stage.getClass());
    String instanceName = name + "_1";
    info = new StageInfo(name, version, instanceName);
    context = new StageContext(instanceName, outputLanes);
    status = Status.CREATED;
  }

  void ensureStatus(Status status) {
    Utils.checkState(this.status == status, Utils.format("Current status '{}', expected '{}'", this.status, status));
  }

  public S getStage() {
    return stage;
  }

  public Stage.Info getInfo() {
    return info;
  }

  public S.Context getContext() {
    return context;
  }

  public void runInit() throws StageException {
    ensureStatus(Status.CREATED);
    stage.init(getInfo(), getContext());
    status = Status.INITIALIZED;
  }

  public void runDestroy() throws StageException {
    ensureStatus(Status.INITIALIZED);
    stage.destroy();
    status = Status.DESTROYED;
  }

  public List<Record> getErrorRecords() {
    return context.getErrorSink().getErrorRecords(info.getInstanceName());
  }

  public List<String> getErrors() {
    List<ErrorMessage> errors = context.getErrorSink().getStageErrors(info.getInstanceName());
    List<String> list = new ArrayList<>();
    for (ErrorMessage error : errors) {
      list.add(error.getNonLocalized());
    }
    return list;
  }

  public void clearErrors() {
    context.getErrorSink().clear();
  }

  public static class Output {
    private final String newOffset;
    private final Map<String, List<Record>> records;

    Output(String newOffset, Map<String, List<Record>> records) {
      this.newOffset = newOffset;
      for (Map.Entry<String, List<Record>> entry : records.entrySet()) {
        entry.setValue(Collections.unmodifiableList(entry.getValue()));
      }
      this.records = Collections.unmodifiableMap(records);
    }

    public String getNewOffset() {
      return newOffset;
    }

    public Map<String, List<Record>> getRecords() {
      return records;
    }
  }

  public static abstract class Builder<S extends Stage, R extends StageRunner, B extends Builder> {
    final S stage;
    final Class<S> stageClass;
    final Set<String> outputLanes;
    final Map<String, Object> configs;

    private Builder(Class<S> stageClass, S stage) {
      this.stageClass =stageClass;
      this.stage = stage;
      outputLanes = new HashSet<>();
      configs = new HashMap<>();
    }

    protected Builder(S stage) {
      this(null, Utils.checkNotNull(stage, "stage"));
    }

    protected Builder(Class<S> stageClass) {
      this(Utils.checkNotNull(stageClass, "stageClass"), null);
    }

    @SuppressWarnings("unchecked")
    public B addOutputLane(String lane) {
      outputLanes.add(Utils.checkNotNull(lane, "lane"));
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B addConfiguration(String name, Object value) {
      configs.put(Utils.checkNotNull(name, "name"), value);
      return (B) this;
    }

    public abstract R build();

  }

  static BatchMaker createTestBatchMaker(String... outputLanes) {
    return new BatchMakerImpl(ImmutableSet.copyOf(outputLanes));
  }

  static Output getOutput(BatchMaker batchMaker) {
    return new Output("sdk:offset", ((BatchMakerImpl)batchMaker).getOutput());
  }


}
