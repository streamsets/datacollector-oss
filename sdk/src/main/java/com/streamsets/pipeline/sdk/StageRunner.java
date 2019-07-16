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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.json.JsonMapperImpl;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.service.ServiceDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public abstract class StageRunner<S extends Stage> extends ProtoRunner {
  private static final Logger LOG = LoggerFactory.getLogger(StageRunner.class);

  private final Class<S> stageClass;
  private final S stage;
  private final Stage.Info info;
  private final StageContext context;
  private final List<ServiceRunner> services;
  private final List<LineageEvent> lineageEvents;

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

  private Set<Class> getDeclaredServices(Class<? extends Stage> klass) {
    StageDef def = getStageDefinition(klass);
    Set<Class> declaredServices = new HashSet<>();
    for(ServiceDependency service : def.services()) {
      declaredServices.add(service.service());
    }
    return declaredServices;
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
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
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
      runtimeInfo,
      services
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
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
  ) {

    if(DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY) == null) {
      DataCollectorServices.instance().put(JsonMapper.SERVICE_KEY, new JsonMapperImpl());
    }

    Utils.checkNotNull(stage, "stage");
    Utils.checkNotNull(configuration, "configuration");
    Utils.checkNotNull(outputLanes, "outputLanes");
    Utils.checkState(getStageDefinition(stageClass) != null, Utils.format("@StageDef annotation not found on class {} (provided the right DStage as stageClass argument?)", stageClass.toString()));
    this.stageClass = stageClass;
    this.stage = stage;
    try {
      configureObject(stage, configuration);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String name = getName(stage.getClass());
    int version = getVersion(stageClass);
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

    // Prepare services structure
    this.services = services;
    Map<Class, Object> serviceMap = new HashMap<>();
    for(ServiceRunner serviceRunner : services) {
      serviceMap.put(serviceRunner.getServiceClass(), serviceRunner.getService());
    }

    // Validate that we have all the services that are needed for the stage proper execution
    Set<Class> declaredServices = getDeclaredServices(stageClass);
    Set<Class> givenServices = serviceMap.keySet();
    Set<Class> missingServices = Sets.difference(declaredServices, givenServices);
    Set<Class> extraServices = Sets.difference(givenServices, declaredServices);
    if(!missingServices.isEmpty() || !extraServices.isEmpty()) {
      throw new RuntimeException(Utils.format("Services mismatch - missing ({}), extra({})", missingServices, extraServices));
    }

    // Create StageContext instance
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
        runtimeInfo,
        serviceMap
    );
    context.getErrorSink().registerInterceptorsForStage(getInfo().getInstanceName(), Collections.emptyList());
    context.getEventSink().registerInterceptorsForStage(getInfo().getInstanceName(), Collections.emptyList());
    status = Status.CREATED;
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
      LOG.debug("Stage '{}' validateConfigs starts", getInfo().getInstanceName());
      ensureStatus(Status.CREATED);
      try {
        // Initialize Services first
        List<Stage.ConfigIssue> issues = new ArrayList<>();
        for(ServiceRunner serviceRunner : services) {
          issues.addAll(serviceRunner.runValidateConfigs());
        }

        // Then the stage itself
        issues.addAll(stage.init(getInfo(), getContext()));

        return issues;
      } finally {
        stage.destroy();
      }
    } finally {
      LOG.debug("Stage '{}' validateConfigs ends", getInfo().getInstanceName());
    }
  }

  @SuppressWarnings("unchecked")
  public void runInit() throws StageException {
    LOG.debug("Stage '{}' init starts", getInfo().getInstanceName());
    ensureStatus(Status.CREATED);

    // Initialize services first
    for(ServiceRunner serviceRunner : services) {
      serviceRunner.runInit();
    }

    // Then the stage itself
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
    // Running destroy on CREATED is valid, because the initialization could have failed (leaving the state in INITALIZED)
    // and even main execution could run destory() on component that never run init() - especially in multi-threaded
    // pipelines.
    ensureStatus(Status.INITIALIZED, Status.CREATED);

    for(ServiceRunner serviceRunner : services) {
      serviceRunner.runDestroy();
    }

    stage.destroy();
    status = Status.DESTROYED;
    LOG.debug("Stage '{}' destroy ends", getInfo().getInstanceName());
  }

  public List<Record> getErrorRecords() throws StageException {
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

  public List<EventRecord> getEventRecords() throws StageException {
    return context.getEventSink().getStageEventsAsEventRecords(info.getInstanceName());
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

  public SourceResponseSink getSourceResponseSink() {
    return context.getSourceResponseSink();
  }

  public void setSourceResponseSink(SourceResponseSink sourceResponseSink) {
    context.setSourceResponseSink(sourceResponseSink);
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
    List<ServiceRunner> services;

    protected Builder(Class<S> stageClass, S stage) {
      this.stageClass =stageClass;
      this.stage = stage;
      outputLanes = new ArrayList<>();
      configs = new HashMap<>();
      onRecordError = OnRecordError.STOP_PIPELINE;
      this.constants = new HashMap<>();
      this.stageSdcConf = new HashMap<>();
      this.runtimeInfo = new SdkRuntimeInfo("", null,  null);
      this.services = new ArrayList<>();
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

    public B addService(Class serviceClass, Object service) {
      this.services.add(new ServiceRunner.Builder(serviceClass, service).build());
      return (B) this;
    }

    public B addService(ServiceRunner runner) {
      this.services.add(runner);
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
