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
package com.streamsets.datacollector.execution.runner.cluster;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.cluster.ClusterHelper;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.RetryUtils;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.execution.runner.common.ProductionPipeline;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.IssuesJson;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.updatechecker.UpdateChecker;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Control class to interact with slave pipelines running on cluster. It provides support for starting, stopping and
 * checking status of pipeline. It also registers information about the pipelines running on slaves.
 */
public class ClusterRunner extends AbstractRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterRunner.class);
  static final String APPLICATION_STATE = "cluster.application.state";
  private static final String APPLICATION_STATE_START_TIME = "cluster.application.startTime";
  static final String SLAVE_ERROR_ATTRIBUTE = "cluster.slave.error";


  @Inject PipelineStateStore pipelineStateStore;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;
  @Inject ResourceManager resourceManager;
  @Inject SlaveCallbackManager slaveCallbackManager;
  @Inject LineagePublisherTask lineagePublisherTask;
  @Inject SupportBundleManager supportBundleManager;

  private final String name;
  private final String rev;
  private String pipelineTitle = null;
  private ObjectGraph objectGraph;
  private ClusterHelper clusterHelper;
  private final File tempDir;
  private static final long SUBMIT_TIMEOUT_SECS = 120;
  private ScheduledFuture<?> managerRunnableFuture;
  private ScheduledFuture<?> metricRunnableFuture;
  private volatile boolean isClosed;
  private ScheduledFuture<?> updateCheckerFuture;
  private UpdateChecker updateChecker;
  private MetricsEventRunnable metricsEventRunnable;
  private PipelineConfiguration pipelineConf;
  private int maxRetries;
  private boolean shouldRetry;
  private ScheduledFuture<Void> retryFuture;
  private long rateLimit = -1L;

  private static final Map<PipelineStatus, Set<PipelineStatus>> VALID_TRANSITIONS =
     new ImmutableMap.Builder<PipelineStatus, Set<PipelineStatus>>()
    .put(PipelineStatus.EDITED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STARTING, ImmutableSet.of(PipelineStatus.START_ERROR, PipelineStatus.RUNNING,
      PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.START_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    // cannot transition to disconnecting from Running
    .put(PipelineStatus.RUNNING, ImmutableSet.of(PipelineStatus.CONNECT_ERROR, PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED,
      PipelineStatus.FINISHED, PipelineStatus.KILLED, PipelineStatus.RUN_ERROR, PipelineStatus.RETRY))
    .put(PipelineStatus.RUN_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.RETRY, ImmutableSet.of(PipelineStatus.STARTING, PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED, PipelineStatus.RUN_ERROR))
    .put(PipelineStatus.STOPPING, ImmutableSet.of(PipelineStatus.STOPPED, PipelineStatus.CONNECT_ERROR, PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.FINISHED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.KILLED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.CONNECT_ERROR, ImmutableSet.of(PipelineStatus.RUNNING, PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED,
      PipelineStatus.KILLED, PipelineStatus.FINISHED, PipelineStatus.RUN_ERROR, PipelineStatus.RETRY))
    .put(PipelineStatus.DISCONNECTED, ImmutableSet.of(PipelineStatus.CONNECTING))
    .put(PipelineStatus.CONNECTING, ImmutableSet.of(PipelineStatus.STARTING, PipelineStatus.RUNNING, PipelineStatus.CONNECT_ERROR, PipelineStatus.RETRY,
      PipelineStatus.FINISHED, PipelineStatus.KILLED, PipelineStatus.RUN_ERROR, PipelineStatus.DISCONNECTED))
    .build();

  @VisibleForTesting
  ClusterRunner(String name, String rev, RuntimeInfo runtimeInfo, Configuration configuration,
    PipelineStoreTask pipelineStore, PipelineStateStore pipelineStateStore, StageLibraryTask stageLibrary,
    SafeScheduledExecutorService executorService, ClusterHelper clusterHelper, ResourceManager resourceManager,
    EventListenerManager eventListenerManager, String sdcToken, AclStoreTask aclStoreTask) {
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStateStore = pipelineStateStore;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    this.runnerExecutor = executorService;
    this.name = name;
    this.rev = rev;
    this.tempDir = Files.createTempDir();
    if (clusterHelper == null) {
      this.clusterHelper = new ClusterHelper(runtimeInfo, null, tempDir, configuration);
    } else {
      this.clusterHelper = clusterHelper;
    }
    this.resourceManager = resourceManager;
    this.eventListenerManager = eventListenerManager;
    this.slaveCallbackManager = new SlaveCallbackManager();
    this.slaveCallbackManager.setClusterToken(sdcToken);
    this.aclStoreTask = aclStoreTask;
  }

  @SuppressWarnings("deprecation")
  public ClusterRunner(String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.objectGraph = objectGraph;
    this.objectGraph.inject(this);
    this.tempDir = new File(new File(runtimeInfo.getDataDir(), "temp"), PipelineUtils.
      escapedPipelineName(Utils.format("cluster-pipeline-{}-{}", name, rev)));
    if (!(this.tempDir.mkdirs() || this.tempDir.isDirectory())) {
      throw new IllegalStateException(Utils.format("Could not create temp directory: {}", tempDir));
    }
    this.clusterHelper = new ClusterHelper(runtimeInfo, new SecurityConfiguration(runtimeInfo,
      configuration), tempDir, configuration);
    if (configuration.get(MetricsEventRunnable.REFRESH_INTERVAL_PROPERTY,
      MetricsEventRunnable.REFRESH_INTERVAL_PROPERTY_DEFAULT) > 0) {
      metricsEventRunnable = this.objectGraph.get(MetricsEventRunnable.class);
    }
    try {
      // CLUSTER is old state, upgrade to cluster batch or cluster streaming based on source
      if (getState().getExecutionMode() == ExecutionMode.CLUSTER) {
        String sourceName = null;
        PipelineConfiguration pipelineConf = getPipelineConf(name, rev);
        for (StageConfiguration stageConf : pipelineConf.getStages()) {
          if (stageConf.getInputLanes().isEmpty()) {
            sourceName = stageConf.getStageName();
            break;
          }
        }
        String msg;
        ExecutionMode executionMode;
        Utils.checkNotNull(sourceName, "Source name should not be null");
        if (sourceName.contains("ClusterHdfsDSource")) {
          msg = "Upgrading execution mode to " + ExecutionMode.CLUSTER_BATCH + " from " + ExecutionMode.CLUSTER;
          executionMode = ExecutionMode.CLUSTER_BATCH;
        } else {
          msg = "Upgrading execution mode to " + ExecutionMode.CLUSTER_YARN_STREAMING + " from " + ExecutionMode.CLUSTER;
          executionMode = ExecutionMode.CLUSTER_YARN_STREAMING;

        }
        PipelineState currentState = getState();
        pipelineStateStore.saveState(currentState.getUser(), name, rev, currentState.getStatus(), msg, currentState.getAttributes(),
          executionMode, currentState.getMetrics(), currentState.getRetryAttempt(),
          currentState.getNextRetryTimeStamp());
      }
    } catch (PipelineException pex) {
      throw new RuntimeException("Error while accessing Pipeline State: " + pex, pex);
    }
  }

  @Override
  public void prepareForDataCollectorStart(String user) throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
    String msg;
    switch (status) {
      case STARTING:
        msg = "Pipeline was in STARTING state, forcing it to DISCONNECTED";
        break;
      case RETRY:
        msg = "Pipeline was in RETRY state, forcing it to DISCONNECTING";
        break;
      case CONNECTING:
        msg = "Pipeline was in CONNECTING state, forcing it to DISCONNECTED";
        break;
      case RUNNING:
        msg = "Pipeline was in RUNNING state, forcing it to DISCONNECTED";
        break;
      case CONNECT_ERROR:
        msg = "Pipeline was in CONNECT_ERROR state, forcing it to DISCONNECTED";
        break;
      case STOPPING:
        msg = "Pipeline was in STOPPING state, forcing it to DISCONNECTED";
        break;
      case DISCONNECTED:
      case EDITED:
      case FINISHED:
      case KILLED:
      case START_ERROR:
      case STOPPED:
        return;
      default:
        throw new IllegalStateException(Utils.format("Pipeline in undefined state: '{}'", status));
    }
    LOG.debug(msg);
    validateAndSetStateTransition(user, PipelineStatus.DISCONNECTED, msg);
  }

  @Override
  public void onDataCollectorStart(String user) throws PipelineException, StageException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
    switch (status) {
      case DISCONNECTED:
        String msg = "Pipeline was in DISCONNECTED state, changing it to CONNECTING";
        LOG.debug(msg);
        validateAndSetStateTransition(user, PipelineStatus.CONNECTING, msg);
        connectOrStart(user);
        break;
      default:
        LOG.error(Utils.format("Pipeline has unexpected status: '{}' on data collector start", status));
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public String getPipelineTitle() throws PipelineException {
    if (pipelineTitle == null) {
      PipelineInfo pipelineInfo = pipelineStore.getInfo(name);
      pipelineTitle = pipelineInfo.getTitle();
    }
    return pipelineTitle;
  }

  @Override
  public void resetOffset(String user) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SourceOffset getCommittedOffsets() throws PipelineException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onDataCollectorStop(String user) throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    stopPipeline(user, true);
  }

  @Override
  public synchronized void stop(String user) throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    stopPipeline(user, false);
  }

  @Override
  public synchronized void forceQuit(String user) throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    throw new UnsupportedOperationException("ForceQuit is not supported in Cluster mode");
  }

  @SuppressWarnings("unchecked")
  private synchronized void stopPipeline(String user, boolean isNodeShuttingDown) throws PipelineStoreException,
    PipelineRunnerException, PipelineRuntimeException {
    try {
      if (isNodeShuttingDown) {
        if (getState().getStatus() == PipelineStatus.RETRY) {
          retryFuture.cancel(true);
        }
        validateAndSetStateTransition(user, PipelineStatus.DISCONNECTED, "Node is shutting down, disconnecting from the "
          + "pipeline in " + getState().getExecutionMode() + " mode");
      } else {
        ApplicationState appState = new ApplicationState((Map) getState().getAttributes().get(APPLICATION_STATE));
        if (appState.getId() == null && getState().getStatus() != PipelineStatus.STOPPED) {
          throw new PipelineRunnerException(ContainerError.CONTAINER_0101, "for cluster application");
        }
        stop(user, appState, pipelineConf);
      }
    } finally {
      cancelRunnable();
    }
  }

  private Map<String, Object> getAttributes() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev).getAttributes();
  }

  @SuppressWarnings("unchecked")
  private void connectOrStart(String user) throws PipelineException,
      StageException {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.putAll(getAttributes());
    ApplicationState appState = new ApplicationState((Map) attributes.get(APPLICATION_STATE));
    if (appState.getId() == null) {
      retryOrStart(user);
    } else {
      try {
        slaveCallbackManager.setClusterToken(appState.getSdcToken());
        pipelineConf = getPipelineConf(name, rev);
      } catch (PipelineRunnerException | PipelineStoreException e) {
        validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, e.toString(), attributes);
        throw e;
      }
      connect(user, appState, pipelineConf);
      if (getState().getStatus().isActive()) {
        scheduleRunnable(user, pipelineConf);
      }
    }
  }

  private void retryOrStart(String user) throws PipelineException, StageException {
    PipelineState pipelineState = getState();
    if (pipelineState.getRetryAttempt() == 0) {
      prepareForStart(user);
      start(user);
    } else {
      validateAndSetStateTransition(user, PipelineStatus.RETRY, "Changing the state to RETRY on startup");
    }
  }

  @Override
  public void prepareForStart(String user) throws PipelineStoreException, PipelineRunnerException {
    PipelineState fromState = getState();
    checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(PipelineStatus.STARTING), ContainerError.CONTAINER_0102,
        fromState.getStatus(), PipelineStatus.STARTING);
    if(!resourceManager.requestRunnerResources(ThreadUsage.CLUSTER)) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0166, name);
    }
    LOG.info("Preparing to start pipeline '{}::{}'", name, rev);
    validateAndSetStateTransition(user, PipelineStatus.STARTING, "Starting pipeline in " + getState().getExecutionMode() + " mode");
  }

  @Override
  public void prepareForStop(String user) throws PipelineStoreException, PipelineRunnerException {
    LOG.info("Preparing to stop pipeline '{}::{}'", name, rev);
    if (getState().getStatus() == PipelineStatus.RETRY) {
      retryFuture.cancel(true);
      validateAndSetStateTransition(user, PipelineStatus.STOPPING, null);
      validateAndSetStateTransition(user, PipelineStatus.STOPPED, "Stopped while the pipeline was in RETRY state");
    } else {
      validateAndSetStateTransition(user, PipelineStatus.STOPPING, "Stopping pipeline in " + getState().getExecutionMode()
        + " mode");
    }
  }

  @Override
  public synchronized void start(String user, Map<String, Object> runtimeParameters) throws PipelineException, StageException {
    // runtimeParameters is not used in Cluster Mode for now
    try {
      Utils.checkState(!isClosed,
        Utils.formatL("Cannot start the pipeline '{}::{}' as the runner is already closed", name, rev));
      ExecutionMode executionMode = pipelineStateStore.getState(name, rev).getExecutionMode();
      if (executionMode != ExecutionMode.CLUSTER_BATCH && executionMode != ExecutionMode.CLUSTER_YARN_STREAMING
          && executionMode != ExecutionMode.CLUSTER_MESOS_STREAMING) {
        throw new PipelineRunnerException(ValidationError.VALIDATION_0073);
      }
      LOG.debug("State of pipeline for '{}::{}' is '{}' ", name, rev, getState());
      pipelineConf = getPipelineConf(name, rev);
      doStart(user, pipelineConf, getClusterSourceInfo(user, name, rev, pipelineConf), getAcl(name));
    } catch (Exception e) {
      validateAndSetStateTransition(user, PipelineStatus.START_ERROR, e.toString(), getAttributes());
      throw e;
    }
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev);
  }

  @Override
  public void startAndCaptureSnapshot(
      String user,
      Map<String, Object> runtimeParameters,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException, StageException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String captureSnapshot(String runner, String name, String label, int batches, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Snapshot getSnapshot(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    return Collections.emptyList();
  }

  @Override
  public void deleteSnapshot(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return pipelineStateStore.getHistory(name, rev, false);
  }

  @Override
  public void deleteHistory() {
    pipelineStateStore.deleteHistory(name ,rev);
  }

  @Override
  public Object getMetrics() {
    if (metricsEventRunnable != null) {
      return metricsEventRunnable.getAggregatedMetrics();
    }
    return null;
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SampledRecord> getSampledRecords(String sampleId, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return slaveCallbackManager.getSlaveCallbackList(callbackObjectType);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineStoreException {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    isClosed = true;
  }

  private void validateAndSetStateTransition(String user, PipelineStatus toStatus, String message)
    throws PipelineStoreException, PipelineRunnerException {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.putAll(getAttributes());
    validateAndSetStateTransition(user, toStatus, message, attributes);
  }

  @VisibleForTesting
  void validateAndSetStateTransition(String user, PipelineStatus toStatus, String message, Map<String, Object> attributes)
    throws PipelineStoreException, PipelineRunnerException {
    Utils.checkState(attributes!=null, "Attributes cannot be set to null");
    PipelineState fromState = getState();
    if (fromState.getStatus() == toStatus && toStatus != PipelineStatus.STARTING) {
      LOG.debug(Utils.format("Ignoring status '{}' as this is same as current status", fromState.getStatus()));
    } else {
      PipelineState pipelineState;
      synchronized (this) {
        fromState = getState();
        checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(toStatus), ContainerError.CONTAINER_0102,
          fromState.getStatus(), toStatus);
        long nextRetryTimeStamp = fromState.getNextRetryTimeStamp();
        int retryAttempt = fromState.getRetryAttempt();
        if (toStatus == PipelineStatus.RUN_ERROR) {
          handleErrorCallbackFromSlaves(attributes);
        }
        if (toStatus == PipelineStatus.RUN_ERROR && shouldRetry) {
          toStatus = PipelineStatus.RETRY;
          checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(toStatus), ContainerError.CONTAINER_0102,
            fromState.getStatus(), toStatus);
        }
        if (toStatus == PipelineStatus.RETRY && fromState.getStatus() != PipelineStatus.CONNECTING) {
          retryAttempt = fromState.getRetryAttempt() + 1;
          if (retryAttempt > maxRetries && maxRetries != -1) {
            LOG.info("Retry attempt '{}' is greater than max no of retries '{}'", retryAttempt, maxRetries);
            toStatus = PipelineStatus.RUN_ERROR;
            retryAttempt = 0;
            nextRetryTimeStamp = 0;
          } else {
            nextRetryTimeStamp = RetryUtils.getNextRetryTimeStamp(retryAttempt, System.currentTimeMillis());
          }
        } else if (!toStatus.isActive()) {
          retryAttempt = 0;
          nextRetryTimeStamp = 0;
        }
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String metricsJSONStr = null;
        if (!toStatus.isActive() || toStatus == PipelineStatus.DISCONNECTED) {
          Object metrics = getMetrics();
          if (metrics != null) {
            try {
              metricsJSONStr = objectMapper.writer().writeValueAsString(metrics);
            } catch (JsonProcessingException e) {
              throw new PipelineStoreException(ContainerError.CONTAINER_0210, e.toString(), e);
            }
          }
          if (metricsJSONStr == null) {
            metricsJSONStr = getState().getMetrics();
          }
        }
        pipelineState =
          pipelineStateStore.saveState(user, name, rev, toStatus, message, attributes, getState().getExecutionMode(),
            metricsJSONStr, retryAttempt, nextRetryTimeStamp);
        if (toStatus == PipelineStatus.RETRY) {
          retryFuture = scheduleForRetries(user, runnerExecutor);
        }
      }
      // This should be out of sync block
      if (eventListenerManager != null) {
        eventListenerManager.broadcastStateChange(
            fromState,
            pipelineState,
            ThreadUsage.CLUSTER,
            OffsetFileUtil.getOffsets(runtimeInfo, name, rev)
        );
      }
    }
  }

  @VisibleForTesting
  void handleErrorCallbackFromSlaves(Map<String, Object> attributes) {
    Set<String> errorMessages = new HashSet<>();
    for (CallbackInfo callbackInfo : slaveCallbackManager.getSlaveCallbackList(CallbackObjectType.ERROR)) {
      String error = callbackInfo.getCallbackObject();
      //Log dedepulicated messages.
      if (errorMessages.add(error)) {
        LOG.error("Error in Slave Runner:" + error);
      }
    }
    if (attributes != null) {
      attributes.put(SLAVE_ERROR_ATTRIBUTE, errorMessages);
    }
    slaveCallbackManager.clearSlaveList(CallbackObjectType.ERROR);
  }

  private void checkState(boolean expr, ContainerError error, Object... args) throws PipelineRunnerException {
    if (!expr) {
      throw new PipelineRunnerException(error, args);
    }
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    slaveCallbackManager.updateSlaveCallbackInfo(callbackInfo);
  }

  @VisibleForTesting
  ClusterSourceInfo getClusterSourceInfo(String user, String name, String rev, PipelineConfiguration pipelineConf)
    throws PipelineRuntimeException, StageException, PipelineStoreException, PipelineRunnerException {

    ProductionPipeline p = createProductionPipeline(user, name, rev, configuration, pipelineConf);
    Pipeline pipeline = p.getPipeline();
    try {
      List<Issue> issues = pipeline.init(false);
      if (!issues.isEmpty()) {
        PipelineRuntimeException e =
          new PipelineRuntimeException(ContainerError.CONTAINER_0800, name, issues.get(0).getMessage());
        Map<String, Object> attributes = new HashMap<>();
        attributes.putAll(getAttributes());
        attributes.put("issues", new IssuesJson(new Issues(issues)));
        validateAndSetStateTransition(user, PipelineStatus.START_ERROR, issues.get(0).getMessage(), attributes);
        throw e;
      }
    } finally {
      pipeline.destroy(false, PipelineStopReason.UNUSED);
    }
    ProtoSource source = p.getPipeline().getSource();
    ClusterSource clusterSource;
    if (source instanceof ClusterSource) {
      clusterSource = (ClusterSource)source;
    } else {
      throw new RuntimeException(Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
    }

    try {
      int parallelism = clusterSource.getParallelism();
      if (parallelism < 1) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0112);
      }
      return new ClusterSourceInfo(parallelism,
                                   clusterSource.getConfigsToShip());
    } catch (IOException | StageException ex) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0117, ex.toString(), ex);
    }
  }

  static class ClusterSourceInfo {
    private final int parallelism;
    private final Map<String, String> configsToShip;

    ClusterSourceInfo(int parallelism, Map<String, String> configsToShip) {
      this.parallelism = parallelism;
      this.configsToShip = configsToShip;
    }

    int getParallelism() {
      return parallelism;
    }

    Map<String, String> getConfigsToShip() {
      return configsToShip;
    }
 }

  private ProductionPipeline createProductionPipeline(String user, String name, String rev, Configuration configuration,
    PipelineConfiguration pipelineConfiguration) throws PipelineStoreException, PipelineRuntimeException,
    StageException {
    ProductionPipelineRunner runner = new ProductionPipelineRunner(
      name,
      rev,
      supportBundleManager,
      configuration,
      runtimeInfo,
      new MetricRegistry(),
      null,
      null
    );
    if (rateLimit > 0) {
      runner.setRateLimit(rateLimit);
    }
    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(
      name,
      rev,
      configuration,
      runtimeInfo,
      stageLibrary,
      runner,
      null,
      lineagePublisherTask
    );
    return builder.build(
      new UserContext(user),
      pipelineConfiguration,
      getState().getTimeStamp(),
      null
    );
  }

  static class ManagerRunnable implements Runnable {
    private final ClusterRunner clusterRunner;
    private final PipelineConfiguration pipelineConf;
    private final String runningUser;

    public ManagerRunnable(ClusterRunner clusterRunner, PipelineConfiguration pipelineConf, String runningUser) {
      this.clusterRunner = clusterRunner;
      this.pipelineConf = pipelineConf;
      this.runningUser = runningUser;
    }

    @Override
    public void run() {
      try {
        checkStatus();
      } catch (Throwable throwable) {
        String msg = "Unexpected error: " + throwable;
        LOG.error(msg, throwable);
      }
    }

    @SuppressWarnings("unchecked")
    private void checkStatus() throws PipelineStoreException, PipelineRunnerException {
      if (clusterRunner.getState().getStatus().isActive()) {
        PipelineState ps = clusterRunner.getState();
        ApplicationState appState = new ApplicationState((Map) ps.getAttributes().get(APPLICATION_STATE));
        clusterRunner.connect(runningUser, appState, pipelineConf);
      }
      if (!clusterRunner.getState().getStatus().isActive() || clusterRunner.getState().getStatus() == PipelineStatus.RETRY) {
        LOG.debug(Utils.format("Cancelling the task as the runner is in a non-active state '{}'",
          clusterRunner.getState()));
        clusterRunner.cancelRunnable();
      }
    }
  }

  private void connect(String user, ApplicationState appState, PipelineConfiguration pipelineConf) throws PipelineStoreException,
    PipelineRunnerException {
    ClusterPipelineStatus clusterPipelineState = null;
    String msg = null;
    boolean connected = false;
    try {
      clusterPipelineState = clusterHelper.getStatus(appState, pipelineConf);
      connected = true;
    } catch (IOException ex) {
      msg = "IO Error while trying to check the status of pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to check the status of pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    } catch (Exception ex) {
      msg = "Error getting status of pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    }
    if (connected) {
      if (clusterPipelineState == ClusterPipelineStatus.RUNNING) {
        msg = "Connected to pipeline in cluster mode";
        validateAndSetStateTransition(user, PipelineStatus.RUNNING, msg);
      } else if (clusterPipelineState == ClusterPipelineStatus.FAILED) {
        msg = "Pipeline failed in cluster";
        LOG.debug(msg);
        postTerminate(user, appState, PipelineStatus.RUN_ERROR, msg);
      } else if (clusterPipelineState == ClusterPipelineStatus.KILLED) {
        msg = "Pipeline killed in cluster";
        LOG.debug(msg);
        postTerminate(user, appState, PipelineStatus.KILLED, msg);
      } else if (clusterPipelineState == ClusterPipelineStatus.SUCCEEDED) {
        msg = "Pipeline succeeded in cluster";
        LOG.debug(msg);
        postTerminate(user, appState, PipelineStatus.FINISHED, msg);
      }
    }
  }

  private void postTerminate(
    String user,
    ApplicationState appState,
    PipelineStatus pipelineStatus, String msg
  ) throws PipelineStoreException, PipelineRunnerException {
    Optional<String> dirID = appState.getDirId();
    // For mesos, remove dir hosting jar once job terminates
    if (dirID.isPresent()) {
      deleteDir(dirID.get());
    }
    Map<String, Object> attributes = new HashMap<String, Object>();
    attributes.putAll(getAttributes());
    attributes.remove(APPLICATION_STATE);
    attributes.remove(APPLICATION_STATE_START_TIME);
    validateAndSetStateTransition(user, pipelineStatus, msg, attributes);
  }

  private void deleteDir(String dirId) {
    File hostingDir = new File(runtimeInfo.getDataDir(), dirId);
    FileUtils.deleteQuietly(hostingDir);
  }

  private synchronized void doStart(
      String user,
      PipelineConfiguration pipelineConf,
      ClusterSourceInfo clusterSourceInfo,
      Acl acl
  ) throws PipelineStoreException, PipelineRunnerException {
    String msg;
    try {
      Utils.checkNotNull(pipelineConf, "PipelineConfiguration cannot be null");
      Utils.checkState(clusterSourceInfo.getParallelism() != 0, "Parallelism cannot be zero");
      if(metricsEventRunnable != null) {
        metricsEventRunnable.clearSlaveMetrics();
      }
      List<Issue> errors = new ArrayList<>();
      PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipelineConf, errors, null);
      if (pipelineConfigBean == null) {
        throw new PipelineRunnerException(ContainerError.CONTAINER_0116, errors);
      }
      maxRetries = pipelineConfigBean.retryAttempts;
      shouldRetry = pipelineConfigBean.shouldRetry;
      rateLimit = pipelineConfigBean.rateLimit;
      registerEmailNotifierIfRequired(pipelineConfigBean, name, pipelineConf.getTitle(),rev);
      registerWebhookNotifierIfRequired(pipelineConfigBean, name, pipelineConf.getTitle(), rev);

      Map<String, String> environment = new HashMap<>(pipelineConfigBean.clusterLauncherEnv);
      Map<String, String> sourceInfo = new HashMap<>();
      File bootstrapDir = new File(this.runtimeInfo.getLibexecDir(), "bootstrap-libs");
      // create pipeline and get the parallelism info from the source
      sourceInfo.put(ClusterModeConstants.NUM_EXECUTORS_KEY, String.valueOf(clusterSourceInfo.getParallelism()));
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_NAME, name);
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_TITLE, pipelineConf.getTitle());
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_REV, rev);
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_USER, user);
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_REMOTE, String.valueOf(isRemotePipeline()));
      for (Map.Entry<String, String> configsToShip : clusterSourceInfo.getConfigsToShip().entrySet()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Config to ship " + configsToShip.getKey() + " = " + configsToShip.getValue());
        }
        sourceInfo.put(configsToShip.getKey(), configsToShip.getValue());
      }
      // This is needed for UI
      runtimeInfo.setAttribute(ClusterModeConstants.NUM_EXECUTORS_KEY, clusterSourceInfo.getParallelism());
      slaveCallbackManager.clearSlaveList();
      ApplicationState applicationState = clusterHelper.submit(pipelineConf, stageLibrary, new File(runtimeInfo.getConfigDir()),
          new File(runtimeInfo.getResourcesDir()), new File(runtimeInfo.getStaticWebDir()), bootstrapDir, environment,
          sourceInfo, SUBMIT_TIMEOUT_SECS, getRules(), acl);
      // set state of running before adding callback which modified attributes
      Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(getAttributes());
      attributes.put(APPLICATION_STATE, applicationState.getMap());
      attributes.put(APPLICATION_STATE_START_TIME, System.currentTimeMillis());
      slaveCallbackManager.setClusterToken(applicationState.getSdcToken());
      validateAndSetStateTransition(
        user,
        PipelineStatus.RUNNING,
        Utils.format("Pipeline in cluster is running ({})", applicationState.getId()),
        attributes
      );
      scheduleRunnable(user, pipelineConf);
    } catch (IOException ex) {
      msg = "IO Error while trying to start the pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.START_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to start the pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.START_ERROR, msg);
    } catch (Exception ex) {
      msg = "Unexpected error starting pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.START_ERROR, msg);
    }
  }

  private void scheduleRunnable(String user, PipelineConfiguration pipelineConf) {
    updateChecker = new UpdateChecker(runtimeInfo, configuration, pipelineConf, this);
    updateCheckerFuture = runnerExecutor.scheduleAtFixedRate(updateChecker, 1, 24 * 60, TimeUnit.MINUTES);
    if(metricsEventRunnable != null) {
      metricRunnableFuture =
        runnerExecutor.scheduleAtFixedRate(metricsEventRunnable, 0, metricsEventRunnable.getScheduledDelay(),
          TimeUnit.MILLISECONDS);
    }
    managerRunnableFuture =
      runnerExecutor.scheduleAtFixedRate(new ManagerRunnable(this, pipelineConf, user), 0, 30, TimeUnit.SECONDS);
  }

  private void cancelRunnable() {
    if (metricRunnableFuture != null) {
      metricRunnableFuture.cancel(true);
      metricsEventRunnable.clearSlaveMetrics();
    }
    if (managerRunnableFuture != null) {
      managerRunnableFuture.cancel(false);
    }
    if (updateCheckerFuture != null) {
      updateCheckerFuture.cancel(true);
    }
  }

  private synchronized void stop(String user, ApplicationState applicationState, PipelineConfiguration pipelineConf)
    throws PipelineStoreException, PipelineRunnerException {
    Utils.checkState(applicationState != null, "Application state cannot be null");
    boolean stopped = false;
    String msg;
    try {
      clusterHelper.kill(applicationState, pipelineConf);
      stopped = true;
    } catch (IOException ex) {
      msg = "IO Error while trying to stop the pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to stop the pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    } catch (Exception ex) {
      msg = "Unexpected error stopping pipeline: " + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
    }
    Map<String, Object> attributes = new HashMap<>();
    if (stopped) {
      Optional<String> dirID = applicationState.getDirId();
      if (dirID.isPresent()) {
        // For mesos, remove dir hosting jar once job terminates
        deleteDir(dirID.get());
      }
      attributes.putAll(getAttributes());
      attributes.remove(APPLICATION_STATE);
      attributes.remove(APPLICATION_STATE_START_TIME);
      validateAndSetStateTransition(user, PipelineStatus.STOPPED, "Stopped cluster pipeline", attributes);
    }
  }

  @Override
  public Map getUpdateInfo() {
    return updateChecker.getUpdateInfo();
  }

  RuleDefinitions getRules() throws PipelineException {
    return pipelineStore.retrieveRules(name, rev);
  }

  @Override
  public String getToken() {
    return slaveCallbackManager.getClusterToken();
  }

  @Override
  public int getRunnerCount() {
    // as cluster runner is only used for managing the slave pipelines, report 1
    return 1;
  }

}
