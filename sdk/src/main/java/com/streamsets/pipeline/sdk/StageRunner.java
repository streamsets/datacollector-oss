/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.sdk.annotationsprocessor.StageHelper;
import com.streamsets.pipeline.util.ContainerError;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

  @SuppressWarnings("unchecked")
  private void configureStage(S stage, Map<String, Object> configuration) {
    try {
      Set<String> fields = getStageConfigurationFields(stage.getClass());
      Set<String> configs = configuration.keySet();
      if (!fields.equals(configs)) {
        Set<String> missingConfigs = Sets.difference(fields, configs);
        Set<String> extraConfigs = Sets.difference(configs, fields);

        missingConfigs = filterNonActiveConfigurationsFromMissing(stage, configuration, missingConfigs);
        if (missingConfigs.size() + extraConfigs.size() > 0) { //x
          throw new RuntimeException(Utils.format(
              "Invalid stage configuration for '{}', Missing configurations '{}' and invalid configurations '{}'",
              stage.getClass().getName(), missingConfigs, extraConfigs));
        }
      }
      for (Field field : stage.getClass().getFields()) {
        if (field.isAnnotationPresent(ConfigDef.class)) {
          ConfigDef configDef = field.getAnnotation(ConfigDef.class);
          if (isConfigurationActive(configDef, configuration)) {
            if ( configDef.type() != ConfigDef.Type.MAP) {
              field.set(stage, configuration.get(field.getName()));
            } else {
              //we need to handle special case of List of Map elements with key/value entries
              Object value = configuration.get(field.getName());
              if (value != null && value instanceof List) {
                Map map = new HashMap();
                for (Map element : (List<Map>) value) {
                  if (!element.containsKey("key") || !element.containsKey("value")) {
                    throw new RuntimeException(Utils.format("Invalid stage configuration for '{}' Map as list must have" +
                                                            " a List of Maps all with 'key' and 'value' entries",
                                                            field.getName()));
                  }
                  String k = (String) element.get("key");
                  String v = (String) element.get("value");
                  map.put(k, v);
                }
                value = map;
              }
              field.set(stage, value);
            }
          }
        }
      }
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new RuntimeException(ex);
    }
  }

  private Set<String> filterNonActiveConfigurationsFromMissing(S stage, Map<String, Object> configuration,
      Set<String> missingConfigs) {
    missingConfigs = new HashSet<>(missingConfigs);
    Iterator<String> it = missingConfigs.iterator();
    while (it.hasNext()) {
      String name = it.next();
      try {
        Field field = stage.getClass().getField(name);
        if (!isConfigurationActive(field.getAnnotation(ConfigDef.class), configuration)) {
          it.remove();
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return missingConfigs;
  }

  private boolean isConfigurationActive(ConfigDef configDef, Map<String, Object> configuration) {
    String dependsOn = configDef.dependsOn();
    if (!dependsOn.isEmpty()) {
      Object dependsOnValue = configuration.get(dependsOn);
      if (dependsOnValue != null) {
        String valueStr = dependsOnValue.toString();
        for (String trigger : configDef.triggeredByValue()) {
          if (valueStr.equals(trigger)) {
            return true;
          }
        }
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  StageRunner(Class<S> stageClass, StageType stageType, Map<String, Object> configuration, List<String> outputLanes,
      boolean isPreview, OnRecordError onRecordError) {
    this((S) getStage(Utils.checkNotNull(stageClass, "stageClass")), stageType, configuration, outputLanes, isPreview,
         onRecordError);
  }

  StageRunner(S stage, StageType stageType, Map < String, Object > configuration, List< String > outputLanes,
      boolean isPreview, OnRecordError onRecordError) {
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
    info = ContextInfoCreator.createInfo(name, version, instanceName);
    context = new StageContext(instanceName, stageType ,isPreview, onRecordError, outputLanes);
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

  @SuppressWarnings("unchecked")
  public void runInit() throws StageException {
    ensureStatus(Status.CREATED);
    List<Stage.ConfigIssue> issues = stage.validateConfigs(getInfo(), getContext());
    if (!issues.isEmpty()) {
      List<String> list = new ArrayList<>(issues.size());
      for (Stage.ConfigIssue issue : issues) {
        list.add(issue.toString());
      }
      throw new StageException(ContainerError.CONTAINER_0010, list);
    }
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
    final List<String> outputLanes;
    final Map<String, Object> configs;
    boolean isPreview;
    OnRecordError onRecordError;

    private Builder(Class<S> stageClass, S stage) {
      this.stageClass =stageClass;
      this.stage = stage;
      outputLanes = new ArrayList<>();
      configs = new HashMap<>();
      onRecordError = OnRecordError.STOP_PIPELINE;
    }

    protected Builder(S stage) {
      this(null, Utils.checkNotNull(stage, "stage"));
    }

    @SuppressWarnings("unchecked")
    public B setPreview(boolean isPreview) {
      this.isPreview = isPreview;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B setOnRecordError(OnRecordError onRecordError) {
      this.onRecordError = onRecordError;
      return (B) this;
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
    return new BatchMakerImpl(ImmutableList.copyOf(outputLanes));
  }

  static Output getOutput(BatchMaker batchMaker) {
    return new Output("sdk:offset", ((BatchMakerImpl)batchMaker).getOutput());
  }


}
