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
package com.streamsets.pipeline.sdk;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.json.JsonMapperImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public abstract class StageRunner<S extends Stage> {
  private static final Logger LOG = LoggerFactory.getLogger(StageRunner.class);

  static {
    RuntimeInfo runtimeInfo = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Collections.singletonList(StageRunner.class.getClassLoader())
    );
    try {
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  enum Status { CREATED, INITIALIZED, DESTROYED}

  private final Class<S> stageClass;
  private final S stage;
  private final Stage.Info info;
  private final StageContext context;
  private final List<LineageEvent> lineageEvents;
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
    return klass.getName().replace(".", "_");
  }

  private int getVersion(Class<? extends Stage> klass) {
    StageDef def = getStageDefinition(klass);
    return (def != null) ? def.version() : -1;
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

  private Set<String> getComplexFieldConfigs(Class<?> klass) throws Exception {
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
        ConfigDef annotation = field.getAnnotation(ConfigDef.class);
        if (!annotation.required() || !isConfigurationActive(annotation, configuration)) {
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
      return false;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  StageRunner(
    Class<S> stageClass,
    StageType stageType,
    Map<String, Object> configuration,
    List<String> outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo
  ) {
    this(
      stageClass,
      (S) getStage(Utils.checkNotNull(stageClass, "stageClass")),
      stageType,
      configuration,
      outputLanes,
      isPreview,
      onRecordError,
      constants,
      stageSdcConf,
      executionMode,
      deliveryGuarantee,
      resourcesDir,
      runtimeInfo
    );
  }

  StageRunner(
    Class<S> stageClass,
    S stage,
    StageType stageType,
    Map < String, Object > configuration,
    List< String > outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo
  ) {

    if(DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY) == null) {
      DataCollectorServices.instance().put(JsonMapper.SERVICE_KEY, new JsonMapperImpl());
    }

    Utils.checkNotNull(stage, "stage");
    Utils.checkNotNull(configuration, "configuration");
    Utils.checkNotNull(outputLanes, "outputLanes");
    this.stageClass = stageClass;
    this.stage = stage;
    try {
      configureStage(stage, configuration);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String name = getName(stage.getClass());
    int version = getVersion(stage.getClass());
    String instanceName = name + "_1";
    info = ContextInfoCreator.createInfo(name, version, instanceName);
    Map<String, Class<?>[]> configToElDefMap;
    try {
      configToElDefMap = ElUtil.getConfigToElDefMap(stageClass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Configuration sdcConfiguration = new Configuration();
    stageSdcConf.forEach((k, v) -> sdcConfiguration.set("stage.conf_" + k, v));
    this.lineageEvents = new ArrayList<>();

    context = new StageContext(
        instanceName,
        stageType,
        0,
        isPreview,
        onRecordError,
        outputLanes,
        configToElDefMap,
        constants,
        executionMode,
        deliveryGuarantee,
        resourcesDir,
        new EmailSender(new Configuration()),
        sdcConfiguration,
        new LineagePublisherDelegator.ListDelegator(this.lineageEvents),
        runtimeInfo
    );
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

  public Stage.Context getContext() {
    return context;
  }

  @SuppressWarnings("unchecked")
  public List<Stage.ConfigIssue> runValidateConfigs() throws StageException {
    try {
      LOG.debug("Stage '{}' validateConfgis starts", getInfo().getInstanceName());
      ensureStatus(Status.CREATED);
      try {
        return stage.init(getInfo(), getContext());
      } finally {
        stage.destroy();
      }
    } finally {
      LOG.debug("Stage '{}' validateConfigs starts", getInfo().getInstanceName());
    }
  }

  @SuppressWarnings("unchecked")
  public void runInit() throws StageException {
    LOG.debug("Stage '{}' init starts", getInfo().getInstanceName());
    ensureStatus(Status.CREATED);
    List<Stage.ConfigIssue> issues = stage.init(getInfo(), getContext());
    if (!issues.isEmpty()) {
      List<String> list = new ArrayList<>(issues.size());
      for (Stage.ConfigIssue issue : issues) {
        list.add(issue.toString());
      }
      throw new StageException(ContainerError.CONTAINER_0010, list);
    }
    status = Status.INITIALIZED;
    LOG.debug("Stage '{}' init ends", getInfo().getInstanceName());
  }

  public void runDestroy() throws StageException {
    LOG.debug("Stage '{}' destroy starts", getInfo().getInstanceName());
    ensureStatus(Status.INITIALIZED);
    stage.destroy();
    status = Status.DESTROYED;
    LOG.debug("Stage '{}' destroy ends", getInfo().getInstanceName());
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

  public List<Record> getEventRecords() {
    return context.getEventSink().getStageEvents(info.getInstanceName());
  }

  public void clearEvents() {
    context.getEventSink().clear();
  }

  public List<LineageEvent> getLineageEvents() {
    return ImmutableList.copyOf(this.lineageEvents);
  }

  public void clearLineageEvents() {
    this.lineageEvents.clear();
  }

  public static class Output {
    private final String offsetEntity;
    private final String newOffset;
    private final Map<String, List<Record>> records;

    Output(String offsetEntity, String newOffset, Map<String, List<Record>> records) {
      this.offsetEntity = offsetEntity;
      this.newOffset = newOffset;
      for (Map.Entry<String, List<Record>> entry : records.entrySet()) {
        entry.setValue(Collections.unmodifiableList(entry.getValue()));
      }
      this.records = Collections.unmodifiableMap(records);
    }

    public String getOffsetEntity() {
      return offsetEntity;
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
    final Map<String, String> stageSdcConf;
    final Map<String, Object> configs;
    final Map<String, Object> constants;
    boolean isPreview;
    ExecutionMode executionMode = ExecutionMode.STANDALONE;
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    OnRecordError onRecordError;
    String resourcesDir;
    RuntimeInfo runtimeInfo;

    protected Builder(Class<S> stageClass, S stage) {
      this.stageClass =stageClass;
      this.stage = stage;
      outputLanes = new ArrayList<>();
      configs = new HashMap<>();
      onRecordError = OnRecordError.STOP_PIPELINE;
      this.constants = new HashMap<>();
      this.stageSdcConf = new HashMap<>();
      this.runtimeInfo = new SdkRuntimeInfo("", null,  null);
    }

    @SuppressWarnings("unchecked")
    public B setPreview(boolean isPreview) {
      this.isPreview = isPreview;
      return (B) this;
    }

    public B setExecutionMode(ExecutionMode executionMode) {
      this.executionMode = executionMode;
      return (B) this;
    }

    public B setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
      this.deliveryGuarantee = deliveryGuarantee;
      return (B) this;
    }

    public B setResourcesDir(String resourcesDir) {
      this.resourcesDir = resourcesDir;
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

    @SuppressWarnings("unchecked")
    public B addStageSdcConfiguration(String name, String value) {
      stageSdcConf.put(Utils.checkNotNull(name, "name"), value);
      return (B) this;
    }

    public B addConstants(Map<String, Object> constants) {
      this.constants.putAll(constants);
      return (B) this;
    }

    public B setRuntimeInfo(RuntimeInfo runtimeInfo) {
      this.runtimeInfo = runtimeInfo;
      return (B) this;
    }

    public abstract R build();

  }

  static BatchMaker createTestBatchMaker(String... outputLanes) {
    return new BatchMakerImpl(ImmutableList.copyOf(outputLanes));
  }

  static Output getOutput(BatchMaker batchMaker) {
    return getOutput(Source.POLL_SOURCE_OFFSET_KEY, "sdk:offset", batchMaker);
  }

  static Output getOutput(String offsetEntity, String offset, BatchMaker batchMaker) {
    return new Output(offsetEntity, offset, ((BatchMakerImpl)batchMaker).getOutput());
  }


}
