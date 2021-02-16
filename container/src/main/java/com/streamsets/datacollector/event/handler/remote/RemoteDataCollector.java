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
package com.streamsets.datacollector.event.handler.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.impl.EventClientImpl;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.dto.WorkerInfo;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.log.LogConstants;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask.sendPipelineMetrics;

public class RemoteDataCollector implements DataCollector {

  public static final String IS_REMOTE_PIPELINE = "IS_REMOTE_PIPELINE";
  public static final String SCH_GENERATED_PIPELINE_NAME = "SCH_GENERATED_PIPELINE_NAME";
  private static final String NAME_AND_REV_SEPARATOR = "::";
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDataCollector.class);
  public static final String JOB_METRICS_URL = "jobrunner/rest/v1/jobs/metrics";
  public static final String MESSAGING_EVENTS_URL = "messaging/rest/v1/events";
  private final Configuration configuration;
  private final Manager manager;
  private final PipelineStoreTask pipelineStore;
  private final List<String> validatorIdList;
  private final PipelineStateStore pipelineStateStore;
  private final RemoteStateEventListener stateEventListener;
  private final AclStoreTask aclStoreTask;
  private final AclCacheHelper aclCacheHelper;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final StageLibraryTask stageLibrary;
  private final BlobStoreTask blobStoreTask;
  private final SafeScheduledExecutorService eventHandlerExecutor;
  private final EventClient eventClient;
  private String jobRunnerMetricsUrl;
  private final Map<String, String> requestHeader;
  protected static final String SEND_METRIC_ATTEMPTS =  "pipeline.metrics.attempts";
  protected static final int DEFAULT_SEND_METRIC_ATTEMPTS = 0;


  @Inject
  public RemoteDataCollector(
      Configuration configuration,
      Manager manager,
      PipelineStoreTask pipelineStore,
      PipelineStateStore pipelineStateStore,
      AclStoreTask aclStoreTask,
      RemoteStateEventListener stateEventListener,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      AclCacheHelper aclCacheHelper,
      StageLibraryTask stageLibrary,
      BlobStoreTask blobStoreTask,
      @Named("eventHandlerExecutor")  SafeScheduledExecutorService eventHandlerExecutor
  ) {
    this.configuration = configuration;
    this.manager = manager;
    this.pipelineStore = pipelineStore;
    this.pipelineStateStore = pipelineStateStore;
    this.validatorIdList = new ArrayList<>();
    this.stateEventListener = stateEventListener;
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.aclStoreTask = aclStoreTask;
    this.aclCacheHelper = aclCacheHelper;
    this.stageLibrary = stageLibrary;
    this.blobStoreTask = blobStoreTask;
    this.eventHandlerExecutor = eventHandlerExecutor;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);
    requestHeader = new HashMap<>();
    requestHeader.put(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
    jobRunnerMetricsUrl = JOB_METRICS_URL;
    eventClient = new EventClientImpl(configuration, () -> runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY));
  }

  PipelineStoreTask getPipelineStoreTask() {
    return pipelineStore;
  }

  @Override
  public void init() {
    stateEventListener.init();
    this.manager.addStateEventListener(stateEventListener);
  }

  @Override
  public void start(Runner.StartPipelineContext context, String name, String rev, Set<String> groups) throws PipelineException, StageException {
    try {
      Callable<String> callable = () -> {
        PipelineState pipelineState = pipelineStateStore.getState(name, rev);
        if (pipelineState.getStatus().isActive()) {
          LOG.warn(
              "Pipeline {}:{} is already in active state {}",
              pipelineState.getPipelineId(),
              pipelineState.getRev(),
              pipelineState.getStatus()
          );
        } else {
          MDC.put(LogConstants.USER, context.getUser());
          PipelineInfo pipelineInfo = pipelineStore.getInfo(name);
          LogUtil.injectPipelineInMDC(pipelineInfo.getTitle(), name);
          manager.getRunner(name, rev).start(context);
        }
        return null;
      };
      if (groups != null) {
        GroupsInScope.execute(groups, callable);
      } else {
        // For SCH versions < 3.16.0, we don't send user groups so we need to just revert back to old functionality of not checking group credentials
        GroupsInScope.executeIgnoreGroups(callable);
      }
    } catch (Exception ex) {
      LOG.warn(Utils.format("Error while starting pipeline: {} is {}", name, ex), ex);
      if (ex.getCause() != null) {
        ExceptionUtils.throwUndeclared(ex.getCause());
      } else {
        ExceptionUtils.throwUndeclared(ex);
      }
    } finally {
      MDC.clear();
    }
  }

  @Override
  public void stop(String user, String name, String rev) throws PipelineException {
    manager.getRunner(name, rev).stop(user);
  }

  @Override
  public void delete(String name, String rev) throws PipelineException {
    pipelineStore.delete(name);
    pipelineStore.deleteRules(name);
  }

  @Override
  public void deleteHistory(String user, String name, String rev) throws PipelineException {
    manager.getRunner(name, rev).deleteHistory();
  }

  @VisibleForTesting
  boolean pipelineStateExists(String name, String rev) throws PipelineException {
    try {
      pipelineStateStore.getState(name, rev);
      return true;
    } catch (PipelineStoreException e) {
      if (e.getErrorCode().getCode().equals(ContainerError.CONTAINER_0209.name())) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public String savePipeline(
      String user,
      String name,
      String rev,
      String description,
      SourceOffset offset,
      final PipelineConfiguration pipelineConfiguration,
      RuleDefinitions ruleDefinitions,
      Acl acl,
      Map<String, Object> metadata,
      Map<String, ConnectionConfiguration> connections
  ) {
    UUID uuid = null;
    try {
      uuid = GroupsInScope.executeIgnoreGroups(() -> {
        // Due to some reason, if pipeline folder doesn't exist but state file exists then remove the state file.
        if (!pipelineStore.hasPipeline(name) && pipelineStateExists(name, rev)) {
          LOG.warn("Deleting state file for pipeline {} as pipeline is deleted", name);
          pipelineStateStore.delete(name, rev);
        }
        UUID uuidRet = pipelineStore.create(user, name, name, description, true, false, metadata).getUuid();
        pipelineConfiguration.setUuid(uuidRet);
        pipelineConfiguration.setPipelineId(name);
        PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary,
            buildInfo,
            name,
            pipelineConfiguration,
            user,
            connections
        );
        PipelineConfiguration validatedPipelineConfig = validator.validate();
        //By default encrypt credentials from Remote data collector
        pipelineStore.save(user, name, rev, description, validatedPipelineConfig, true);
        pipelineStore.storeRules(name, rev, ruleDefinitions, false);
        if (acl != null) { // can be null for old dpm or when DPM jobs have no acl
          aclStoreTask.saveAcl(name, acl);
        }
        LOG.info("Offset for remote pipeline '{}:{}' is {}", name, rev, offset);
        if (offset != null) {
          OffsetFileUtil.saveSourceOffset(runtimeInfo, name, rev, offset);
        }
        return uuidRet;
      });
    } catch (Exception ex) {
      LOG.warn(Utils.format("Error while saving pipeline: {} is {}", name, ex), ex);
      if (ex.getCause() != null) {
        ExceptionUtils.throwUndeclared(ex.getCause());
      } else {
        ExceptionUtils.throwUndeclared(ex);
      }
    } finally {
      MDC.clear();
    }
    return uuid.toString();
  }


  @Override
  public void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineException {
    // Check for existence of pipeline first
    pipelineStore.getInfo(name);
    ruleDefinitions.setUuid(pipelineStore.retrieveRules(name, rev).getUuid());
    pipelineStore.storeRules(name, rev, ruleDefinitions, false);
  }

  @Override
  public void resetOffset(String user, String name, String rev) throws PipelineException {
    manager.getRunner(name, rev).resetOffset(user);
  }

  @Override
  public void validateConfigs(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs
  ) throws PipelineException {
    Previewer previewer = manager.createPreviewer(user, name, rev, interceptorConfs, p -> null, false, new HashMap<>());
    previewer.validateConfigs(1000L);
    validatorIdList.add(previewer.getId());
  }

  @Override
  public String previewPipeline(
      String user,
      String name,
      String rev,
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      List<StageOutput> stagesOverride,
      long timeoutMillis,
      boolean testOrigin,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      Map<String, ConnectionConfiguration> connections
  ) {
    String previewerId = null;
    try {
      previewerId = GroupsInScope.executeIgnoreGroups(() -> {
        final Previewer previewer = manager.createPreviewer(user, name, rev, interceptorConfs, afterActionsFunction, false, connections);
        previewer.validateConfigs(timeoutMillis);
        return previewer.getId();
      });
    } catch (Exception ex) {
      LOG.warn(Utils.format("Error while previewing pipeline: {} is {}", name, ex), ex);
      if (ex.getCause() != null) {
        ExceptionUtils.throwUndeclared(ex.getCause());
      } else {
        ExceptionUtils.throwUndeclared(ex);
      }
    } finally {
      MDC.clear();
    }
    return previewerId;
  }

  static class StopAndDeleteCallable implements Callable<AckEvent> {
    private final RemoteDataCollector remoteDataCollector;
    private final String pipelineName;
    private final String rev;
    private final String user;
    private final long forceStopMillis;
    private final int attempts;
    private final EventClient eventClient;
    private String jobRunnerUrl;
    private final Map<String, String> requestHeader;

    public StopAndDeleteCallable(
        RemoteDataCollector remoteDataCollector, String user, String pipelineName, String rev, long forceStopMillis,
        EventClient eventClient, String jobRunnerUrl, Map<String, String> requestHeader, Configuration configuration
    ) {
      this.remoteDataCollector = remoteDataCollector;
      this.pipelineName = pipelineName;
      this.rev = rev;
      this.user = user;
      this.forceStopMillis = forceStopMillis;
      this.requestHeader = requestHeader;
      this.jobRunnerUrl = jobRunnerUrl;
      this.eventClient = eventClient;
      this.attempts = configuration.get(SEND_METRIC_ATTEMPTS, DEFAULT_SEND_METRIC_ATTEMPTS);
    }

    private boolean waitForInactiveState(
        PipelineStateStore pipelineStateStore, long time
    ) throws PipelineStoreException {
      long now = System.currentTimeMillis();
      PipelineState pipelineState = pipelineStateStore.getState(pipelineName, rev);
      while (pipelineState.getStatus().isActive() && (System.currentTimeMillis() - now) < time) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IllegalStateException("Interrupted while waiting for pipeline to stop " + e, e);
        }
        pipelineState = pipelineStateStore.getState(pipelineName, rev);
      }
      return pipelineStateStore.getState(pipelineName, rev).getStatus().isActive();
    }

    @Override
    public AckEvent call() {
      long startTime = System.currentTimeMillis();
      AckEventStatus ackStatus = AckEventStatus.SUCCESS;
      String ackEventMessage = null;
      try {
        if (!remoteDataCollector.pipelineStore.hasPipeline(pipelineName)) {
          LOG.warn("Pipeline {}:{} is already deleted", pipelineName, rev);
          return new AckEvent(ackStatus, ackEventMessage);
        }
        PipelineStateStore pipelineStateStore = remoteDataCollector.pipelineStateStore;
        Manager manager = remoteDataCollector.manager;
        PipelineState pipelineState = pipelineStateStore.getState(pipelineName, rev);
        if (pipelineState.getStatus().equals(PipelineStatus.STOPPING)) {
          throw new RuntimeException("Pipeline is already being stopped by another invocation of stopJob");
        }
        if (pipelineState.getStatus().isActive()) {
          try {
            manager.getRunner(pipelineName, rev).stop(user);
          } catch (Exception e) {
            LOG.warn("Error while stopping the pipeline {}", e, e);
          }
        }

        // wait for forceTimeoutMillis before a force quit
        boolean isActive = waitForInactiveState(pipelineStateStore, forceStopMillis);


        // If still active, force stop of this pipeline as we are deleting this anyways
        if (isActive) {
          try {
            manager.getRunner(pipelineName, rev).forceQuit(user);
          } catch (Exception e) {
            LOG.warn("Cannot issue force quit on pipeline {}", pipelineName);
          }
        }
        // wait for few secs before terminating a force quit
        isActive = waitForInactiveState(pipelineStateStore, 10000);

        // If still active, force change state
        if (isActive) {
          pipelineStateStore.saveState(
              user,
              pipelineName,
              rev,
              PipelineStatus.STOPPED,
              "Stopping pipeline forcefully as we are performing a delete afterwards",
              pipelineState.getAttributes(),
              pipelineState.getExecutionMode(),
              pipelineState.getMetrics(),
              pipelineState.getRetryAttempt(),
              pipelineState.getNextRetryTimeStamp()
          );
        }
        try {
          // We're currently sending with attempts set to 0 because we are disabling this function by default
          sendPipelineMetrics(remoteDataCollector, eventClient, jobRunnerUrl, requestHeader, attempts);
        } catch (IOException | PipelineException ex) {
          // if metrics sending fail, lets log an error and continue with delete of the pipeline
          LOG.error(Utils.format("Cannot send metrics for pipeline: {} due to: {}", pipelineState.getPipelineId(), ex),
              ex
          );
        }
        remoteDataCollector.delete(pipelineName, rev);
      } catch (Exception ex) {
        ackStatus = AckEventStatus.ERROR;
        ackEventMessage = Utils.format("Remote event type {} encountered error {}", EventType.STOP_DELETE_PIPELINE,
            ex);
        LOG.error(ex.getMessage(), ex);
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Time in secs to stop and delete pipeline {} is {}", pipelineName, (endTime - startTime)/1000);
      AckEvent ackEvent = new AckEvent(ackStatus, ackEventMessage);
      return ackEvent;
    }
  }

  @Override
  public Future<AckEvent> stopAndDelete(
      String user, String name, String rev, long forceTimeoutMillis
  ) {
    LOG.info("Pipeline will be stopped and deleted, force timeout is {}", forceTimeoutMillis);
    Future<AckEvent> ackEventFuture = eventHandlerExecutor.submit(new StopAndDeleteCallable(this,
        user,
        name,
        rev,
        forceTimeoutMillis,
        eventClient,
        jobRunnerMetricsUrl,
        requestHeader,
        configuration
    ));
    return ackEventFuture;
  }

  // Returns info about remote pipelines that have changed since the last sending of events
  @Override
  public List<PipelineAndValidationStatus> getRemotePipelinesWithChanges() throws PipelineException {
    List<PipelineAndValidationStatus> pipelineAndValidationStatuses = new ArrayList<>();
    for (Pair<PipelineState, Map<String, String>> pipelineStateAndOffset: stateEventListener.getPipelineStateEvents()) {
      PipelineState pipelineState = pipelineStateAndOffset.getLeft();
      Map<String, String> offset = pipelineStateAndOffset.getRight();
      String name = pipelineState.getPipelineId();
      String rev = pipelineState.getRev();
      boolean isClusterMode = pipelineState.getExecutionMode() != ExecutionMode.STANDALONE &&
          pipelineState.getExecutionMode() != ExecutionMode.BATCH &&
          pipelineState.getExecutionMode() != ExecutionMode.STREAMING;
      List<WorkerInfo> workerInfos = new ArrayList<>();
      String title;
      int runnerCount = 0;
      if (pipelineStore.hasPipeline(name)) {
        title = pipelineStore.getInfo(name).getTitle();
        Runner runner = manager.getRunner(name, rev);
        if (isClusterMode) {
          workerInfos = getWorkers(runner.getSlaveCallbackList(CallbackObjectType.METRICS));
        }
        runnerCount = runner.getRunnerCount();
      } else {
        title = null;
      }
      pipelineAndValidationStatuses.add(new PipelineAndValidationStatus(
          getSchGeneratedPipelineName(pipelineState),
          title,
          rev,
          pipelineState.getTimeStamp(),
          true,
          pipelineState.getStatus(),
          pipelineState.getMessage(),
          workerInfos,
          isClusterMode,
          getSourceOffset(name, offset),
          null,
          runnerCount
      ));
    }
    return pipelineAndValidationStatuses;
  }

  @Override
  public void syncAcl(Acl acl) throws PipelineException {
    if (acl == null) {
      return;
    }
    if (pipelineStore.hasPipeline(acl.getResourceId())) {
      aclStoreTask.saveAcl(acl.getResourceId(), acl);
    } else {
      LOG.warn(ContainerError.CONTAINER_0200.getMessage(), acl.getResourceId());
    }
  }

  @Override
  public void blobStore(String namespace, String id, long version, String content) throws StageException {
    blobStoreTask.store(namespace, id, version, content);
  }

  @Override
  public void blobDelete(String namespace, String id) throws StageException {
    LOG.debug("Deleting all blob objects for namespace={} and id={}", namespace, id);
    blobStoreTask.deleteAllVersions(namespace, id);
  }

  @Override
  public void blobDelete(String namespace, String id, long version) throws StageException {
    blobStoreTask.delete(namespace, id, version);
  }

  @Override
  public void storeConfiguration(Map<String, String> newConfiguration) throws IOException {
    RuntimeInfo.storeControlHubConfigs(runtimeInfo, newConfiguration);
    configuration.set(newConfiguration);
  }

  private List<WorkerInfo> getWorkers(Collection<CallbackInfo> callbackInfos) {
    List<WorkerInfo> workerInfos = new ArrayList<>();
    for (CallbackInfo callbackInfo : callbackInfos) {
      WorkerInfo workerInfo = new WorkerInfo();
      workerInfo.setWorkerURL(callbackInfo.getSdcURL());
      workerInfo.setWorkerId(callbackInfo.getSlaveSdcId());
      workerInfos.add(workerInfo);
    }
    return workerInfos;
  }

  private String getOffset(String pipelineName, String rev) {
    return OffsetFileUtil.getSourceOffset(runtimeInfo, pipelineName, rev);
  }


  String getSchGeneratedPipelineName(String name, String rev) throws PipelineException {
    return getSchGeneratedPipelineName(pipelineStateStore.getState(name, rev));
  }

  private String getSchGeneratedPipelineName(PipelineState pipelineState) {
    // return name with colon so control hub can interpret the job id from the name
    Object schGenName = pipelineState.getAttributes().get(RemoteDataCollector.SCH_GENERATED_PIPELINE_NAME);
    // will be null for pipelines with version earlier than 3.7
    return (schGenName == null) ? pipelineState.getPipelineId() : (String) schGenName;
  }

  @Override
  public Collection<PipelineAndValidationStatus> getPipelines() throws IOException, PipelineException {
    List<PipelineState> pipelineStates = manager.getPipelines();
    Map<String, PipelineAndValidationStatus> pipelineStatusMap = new HashMap<>();
    Set<String> localPipelineIds = new HashSet<>();
    for (PipelineState pipelineState : pipelineStates) {
      boolean isRemote = false;
      String name = pipelineState.getPipelineId();
      PipelineInfo pipelineInfo = pipelineStore.getInfo(name);
      String title = pipelineInfo.getTitle();
      String rev = pipelineState.getRev();
      if (manager.isRemotePipeline(name, rev)) {
        isRemote = true;
      }
      // ignore local and non active pipelines
      if (isRemote || manager.isPipelineActive(name, rev)) {
        List<WorkerInfo> workerInfos = new ArrayList<>();
        boolean isClusterMode = pipelineState.getExecutionMode() != ExecutionMode.STANDALONE &&
            pipelineState.getExecutionMode() != ExecutionMode.BATCH &&
            pipelineState.getExecutionMode() != ExecutionMode.STREAMING;
        Runner runner = manager.getRunner(name, rev);
        if (isClusterMode) {
          for (CallbackInfo callbackInfo : runner.getSlaveCallbackList(CallbackObjectType.METRICS)) {
            WorkerInfo workerInfo = new WorkerInfo();
            workerInfo.setWorkerURL(callbackInfo.getSdcURL());
            workerInfo.setWorkerId(callbackInfo.getSlaveSdcId());
            workerInfos.add(workerInfo);
          }
        }
        Acl acl = null;
        if (!isRemote) { // if remote, dpm owns acl, sdc sends null acl
          localPipelineIds.add(name);
          acl = aclCacheHelper.getAcl(name);
        }
        pipelineStatusMap.put(getNameAndRevString(name, rev), new PipelineAndValidationStatus(
            getSchGeneratedPipelineName(name, rev),
            title,
            rev,
            pipelineState.getTimeStamp(),
            isRemote,
            pipelineState.getStatus(),
            pipelineState.getMessage(),
            workerInfos,
            isClusterMode,
            isRemote ? getOffset(name, rev) : null,
            acl,
            runner.getRunnerCount()
        ));
      }
    }
    aclCacheHelper.removeIfAbsent(localPipelineIds);
    setValidationStatus(pipelineStatusMap);
    return pipelineStatusMap.values();
  }

  public List<PipelineState> getRemotePipelines() throws PipelineException {
    List<PipelineState> pipelineStates = manager.getPipelines();
    List<PipelineState> remoteStates = new ArrayList<>();
    for (PipelineState pipelineState: pipelineStates) {
      if (manager.isRemotePipeline(pipelineState.getPipelineId(), pipelineState.getRev())) {
        remoteStates.add(pipelineState);
      }
    }
    return remoteStates;
  }

  private void setValidationStatus(Map<String, PipelineAndValidationStatus> pipelineStatusMap) {
    List<String> idsToRemove = new ArrayList<>();
    for (String previewerId : validatorIdList) {
      Previewer previewer = manager.getPreviewer(previewerId);
      if (previewer == null) {
        continue;
      }
      ValidationStatus validationStatus = null;
      Issues issues = null;
      String message = null;
      if (previewer != null) {
        PreviewStatus previewStatus = previewer.getStatus();
        switch (previewStatus) {
          case INVALID:
            validationStatus = ValidationStatus.INVALID;
            break;
          case TIMING_OUT:
          case TIMED_OUT:
            validationStatus = ValidationStatus.TIMED_OUT;
            break;
          case VALID:
            validationStatus = ValidationStatus.VALID;
            break;
          case VALIDATING:
            validationStatus = ValidationStatus.VALIDATING;
            break;
          case VALIDATION_ERROR:
            validationStatus = ValidationStatus.VALIDATION_ERROR;
            break;
          default:
            LOG.warn(Utils.format("Unrecognized validation state: '{}'", previewStatus));
        }
        if (!previewStatus.isActive()) {
          PreviewOutput previewOutput = previewer.getOutput();
          issues = previewOutput.getIssues();
          message = previewOutput.getMessage();
          idsToRemove.add(previewerId);
        }
      } else {
        LOG.warn(Utils.format("Previewer is null for id: '{}'", previewerId));
      }
      PipelineAndValidationStatus pipelineAndValidationStatus =
        pipelineStatusMap.get(getNameAndRevString(previewer.getName(), previewer.getRev()));
      if (pipelineAndValidationStatus == null) {
        LOG.warn("Preview pipeline: '{}'::'{}' doesn't exist", previewer.getName(), previewer.getRev());
      } else {
        pipelineAndValidationStatus.setValidationStatus(validationStatus);
        pipelineAndValidationStatus.setIssues(issues);
        pipelineAndValidationStatus.setMessage(message);
      }
    }
    for (String id : idsToRemove) {
      validatorIdList.remove(id);
    }
  }

  private String getNameAndRevString(String name, String rev) {
    return name + NAME_AND_REV_SEPARATOR + rev;
  }

  @VisibleForTesting
  List<String> getValidatorList() {
    return validatorIdList;
  }

  private String getSourceOffset(String pipelineId, Map<String, String> offset) {
    SourceOffset sourceOffset = new SourceOffset(SourceOffset.CURRENT_VERSION, offset);
    try {
      return ObjectMapperFactory.get().writeValueAsString(new SourceOffsetJson(sourceOffset));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(Utils.format("Failed to fetch source offset for pipeline: {} due to error: {}",
          pipelineId,
          e.toString()
      ), e);
    }
  }

  public Runner getRunner(String name, String rev) throws PipelineException  {
    return manager.getRunner(name, rev);
  }
}

