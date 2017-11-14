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
package com.streamsets.datacollector.execution.runner.standalone;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.RetryUtils;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.execution.runner.common.DataObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.MetricObserverRunnable;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.ProductionObserver;
import com.streamsets.datacollector.execution.runner.common.ProductionPipeline;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunnable;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.datacollector.execution.runner.common.RulesConfigLoader;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.execution.runner.common.ThreadHealthReporter;
import com.streamsets.datacollector.execution.runner.common.dagger.PipelineProviderModule;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.updatechecker.UpdateChecker;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.log.LogConstants;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StandaloneRunner extends AbstractRunner implements StateListener {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneRunner.class);
  public static final String STATS_NULL_TARGET = "com_streamsets_pipeline_stage_destination_devnull_StatsNullDTarget";
  public static final String STATS_DPM_DIRECTLY_TARGET =
      "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget";

  private static final ImmutableList<PipelineStatus> RESET_OFFSET_DISALLOWED_STATUSES = ImmutableList.of(
      PipelineStatus.CONNECTING,
      PipelineStatus.DISCONNECTING,
      PipelineStatus.FINISHING,
      PipelineStatus.RETRY,
      PipelineStatus.RUNNING,
      PipelineStatus.STARTING,
      PipelineStatus.STOPPING
  );

  private static final ImmutableSet<PipelineStatus> FORCE_QUIT_ALLOWED_STATES = ImmutableSet.of(
    PipelineStatus.STOPPING,
    PipelineStatus.STOPPING_ERROR,
    PipelineStatus.STARTING_ERROR,
    PipelineStatus.RUNNING_ERROR,
    PipelineStatus.FINISHING
  );

  @Inject PipelineStoreTask pipelineStoreTask;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject SnapshotStore snapshotStore;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;
  @Inject ResourceManager resourceManager;

  private final ObjectGraph objectGraph;
  private final String name;
  private String pipelineTitle = null;
  private final String rev;
  // User context for the user who started the pipeline
  private String token;

  /*Mutex objects to synchronize start and stop pipeline methods*/
  private ThreadHealthReporter threadHealthReporter;
  private DataObserverRunnable observerRunnable;
  private ProductionPipeline prodPipeline;
  private MetricsEventRunnable metricsEventRunnable;
  private int maxRetries;
  private ScheduledFuture<Void> retryFuture;
  private ProductionPipelineRunnable pipelineRunnable;
  private boolean isRetrying;
  private volatile boolean isClosed;
  private UpdateChecker updateChecker;
  private volatile String metricsForRetry;
  private final List<ErrorListener> errorListeners;

  private static final Map<PipelineStatus, Set<PipelineStatus>> VALID_TRANSITIONS =
    new ImmutableMap.Builder<PipelineStatus, Set<PipelineStatus>>()
    .put(PipelineStatus.EDITED, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.STARTING, ImmutableSet.of(
      PipelineStatus.START_ERROR,     // Used when we can't even build runtime structures
      PipelineStatus.STARTING_ERROR,  // Used when runtime structures are built, but then pipeline's init() fails
      PipelineStatus.RUNNING,
      PipelineStatus.DISCONNECTING,
      PipelineStatus.STOPPING
    ))
    .put(PipelineStatus.STARTING_ERROR, ImmutableSet.of(
      PipelineStatus.START_ERROR,
      PipelineStatus.STOPPING_ERROR,
      PipelineStatus.RETRY
    ))
    .put(PipelineStatus.START_ERROR, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.RUNNING, ImmutableSet.of(
      PipelineStatus.RUNNING_ERROR,
      PipelineStatus.FINISHING,
      PipelineStatus.STOPPING,
      PipelineStatus.DISCONNECTING
    ))
    .put(PipelineStatus.RUNNING_ERROR, ImmutableSet.of(
      PipelineStatus.RETRY,
      PipelineStatus.STOPPING_ERROR,
      PipelineStatus.RUN_ERROR
    ))
    .put(PipelineStatus.RETRY, ImmutableSet.of(
      PipelineStatus.STARTING,
      PipelineStatus.STOPPING,
      PipelineStatus.DISCONNECTING
    ))
    .put(PipelineStatus.RUN_ERROR, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.STOPPING_ERROR, ImmutableSet.of(
      PipelineStatus.START_ERROR,
      PipelineStatus.RUN_ERROR,
      PipelineStatus.STOP_ERROR,
      PipelineStatus.RETRY
    ))
    .put(PipelineStatus.STOP_ERROR, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.FINISHING, ImmutableSet.of(
      PipelineStatus.FINISHED,
      PipelineStatus.STOPPING_ERROR
    ))
    .put(PipelineStatus.STOPPING, ImmutableSet.of(
      PipelineStatus.STOPPED,
      PipelineStatus.STOPPING_ERROR
    ))
    .put(PipelineStatus.FINISHED, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.STOPPED, ImmutableSet.of(
      PipelineStatus.STARTING
    ))
    .put(PipelineStatus.DISCONNECTING, ImmutableSet.of(
      PipelineStatus.DISCONNECTED
    ))
    .put(PipelineStatus.DISCONNECTED, ImmutableSet.of(
      PipelineStatus.CONNECTING,
      PipelineStatus.STARTING,
      PipelineStatus.RETRY
    ))
    .put(PipelineStatus.CONNECTING, ImmutableSet.of(
      PipelineStatus.STARTING,
      PipelineStatus.DISCONNECTING,
      PipelineStatus.RETRY
    ))
    .build();

  public StandaloneRunner(String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.objectGraph = objectGraph;
    this.errorListeners = new ArrayList<>();
    objectGraph.inject(this);
  }

  public void addErrorListener(ErrorListener errorListener) {
    this.errorListeners.add(errorListener);
  }

  @Override
  public void prepareForDataCollectorStart(String user) throws PipelineException {
    PipelineStatus status = getState().getStatus();
    try {
      MDC.put(LogConstants.USER, user);
      LogUtil.injectPipelineInMDC(getPipelineTitle(), getName());
      LOG.info("Pipeline " + name + " with rev " + rev + " is in state: " + status);
      String msg = null;
      List<PipelineStatus> transitions = new ArrayList<>();
      switch (status) {
        case STARTING:
          msg = "Pipeline was in STARTING state, forcing it to DISCONNECTING";
          transitions.add(PipelineStatus.DISCONNECTING);
          transitions.add(PipelineStatus.DISCONNECTED);
          break;
        case RETRY:
          msg = "Pipeline was in RETRY state, forcing it to DISCONNECTING";
          transitions.add(PipelineStatus.DISCONNECTING);
          transitions.add(PipelineStatus.DISCONNECTED);
          break;
        case CONNECTING:
          msg = "Pipeline was in CONNECTING state, forcing it to DISCONNECTING";
          transitions.add(PipelineStatus.DISCONNECTING);
          transitions.add(PipelineStatus.DISCONNECTED);
          break;
        case RUNNING:
          msg = "Pipeline was in RUNNING state, forcing it to DISCONNECTING";
          transitions.add(PipelineStatus.DISCONNECTING);
          transitions.add(PipelineStatus.DISCONNECTED);
          break;
        case DISCONNECTING:
          msg = "Pipeline was in DISCONNECTING state, forcing it to DISCONNECTED";
          transitions.add(PipelineStatus.DISCONNECTED);
          break;
        case STARTING_ERROR:
          msg = "Pipeline was in STARTING_ERROR state, forcing it to START_ERROR";
          transitions.add(PipelineStatus.START_ERROR);
          break;
        case STOPPING_ERROR:
          msg = "Pipeline was in STOPPING_ERROR state, forcing it to STOP_ERROR";
          transitions.add(PipelineStatus.STOP_ERROR);
          break;
        case RUNNING_ERROR:
          msg = "Pipeline was in RUNNING_ERROR state, forcing it to terminal state of RUN_ERROR";
          transitions.add(PipelineStatus.RUN_ERROR);
          break;
        case STOPPING:
          msg = "Pipeline was in STOPPING state, forcing it to terminal state of STOPPED";
          transitions.add(PipelineStatus.STOPPED);
          break;
        case FINISHING:
          msg = "Pipeline was in FINISHING state, forcing it to terminal state of FINISHED";
          transitions.add(PipelineStatus.FINISHED);
          break;
        case DISCONNECTED:
        case RUN_ERROR:
        case EDITED:
        case FINISHED:
        case KILLED:
        case START_ERROR:
        case STOPPED:
        case STOP_ERROR:
          break;
        default:
          throw new IllegalStateException(Utils.format("Pipeline in undefined state: '{}'", status));
      }
      if (msg != null) {
        LOG.debug(msg);
      }

      // Perform the transitions
      for (PipelineStatus t : transitions) {
        validateAndSetStateTransition(user, t, msg, null);
      }

    } finally {
      MDC.clear();
    }
  }

  @Override
  public void onDataCollectorStart(String user) throws PipelineException, StageException {
    try {
      MDC.put(LogConstants.USER, user);
      LogUtil.injectPipelineInMDC(getPipelineTitle(), getName());
      PipelineState pipelineState = getState();
      PipelineStatus status = pipelineState.getStatus();
      Map<String, Object> attributes = pipelineState.getAttributes();
      LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
      //if the pipeline was running and capture snapshot in progress, then cancel and delete snapshots
      for(SnapshotInfo snapshotInfo : getSnapshotsInfo()) {
        if(snapshotInfo.isInProgress()) {
          snapshotStore.deleteSnapshot(snapshotInfo.getName(), snapshotInfo.getRev(), snapshotInfo.getId());
        }
      }
      switch (status) {
        case DISCONNECTED:
          String msg = "Pipeline was in DISCONNECTED state, changing it to CONNECTING";
          LOG.debug(msg);
          // Ger Runtime Constants from Pipeline State
          if (attributes != null && attributes.containsKey(ProductionPipeline.RUNTIME_PARAMETERS_ATTR)) {
            runtimeParameters = (Map<String, Object>) attributes.get(ProductionPipeline.RUNTIME_PARAMETERS_ATTR);
          }
          retryOrStart(user);
          break;
        default:
          LOG.error(Utils.format("Pipeline cannot start with status: '{}'", status));
      }
    } finally {
      MDC.clear();
    }
  }

  private void retryOrStart(String user) throws PipelineException, StageException {
    PipelineState pipelineState = getState();
    if (pipelineState.getRetryAttempt() == 0) {
      prepareForStart(user);
      start(user, runtimeParameters);
    } else {
      validateAndSetStateTransition(user, PipelineStatus.RETRY, "Changing the state to RETRY on startup", null);
      isRetrying = true;
      metricsForRetry = getState().getMetrics();
    }
  }

  @Override
  public void onDataCollectorStop(String user) throws PipelineException {
    try {
      MDC.put(LogConstants.USER, user);
      LogUtil.injectPipelineInMDC(getPipelineTitle(), getName());
      if (getState().getStatus() == PipelineStatus.RETRY) {
        LOG.info("Pipeline '{}'::'{}' is in retry", name, rev);
        retryFuture.cancel(true);
        validateAndSetStateTransition(user, PipelineStatus.DISCONNECTING, null, null);
        validateAndSetStateTransition(user, PipelineStatus.DISCONNECTED, "Disconnected as SDC is shutting down", null);
        return;
      }
      if (!getState().getStatus().isActive() || getState().getStatus() == PipelineStatus.DISCONNECTED) {
        LOG.info("Pipeline '{}'::'{}' is no longer active", name, rev);
        return;
      }
      LOG.info("Stopping pipeline {}::{}", name, rev);
      try {
        try {
          validateAndSetStateTransition(user, PipelineStatus.DISCONNECTING, "Stopping the pipeline as SDC is shutting down",
                                        null);
        } catch (PipelineRunnerException ex) {
          // its ok if state validation fails - we will still try to stop pipeline if active
          LOG.warn("Cannot transition to PipelineStatus.DISCONNECTING: {}", ex.toString(), ex);
        }
        stopPipeline(true /* shutting down node process */);
      } catch (Exception e) {
        LOG.warn("Error while stopping the pipeline: {} ", e.toString(), e);
      }
    } finally {
      MDC.clear();
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
      PipelineInfo pipelineInfo = pipelineStoreTask.getInfo(name);
      pipelineTitle = pipelineInfo.getTitle();
    }
    return pipelineTitle;
  }

  @Override
  public synchronized void resetOffset(String user) throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    LOG.debug("Resetting offset for pipeline {}, {}", name, rev);
    if (RESET_OFFSET_DISALLOWED_STATUSES.contains(status)) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0104, name);
    }
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    offsetTracker.resetOffset(name, rev);
  }

  public SourceOffset getCommittedOffsets() throws PipelineException {
    return OffsetFileUtil.getOffset(runtimeInfo, name, rev);
  }

  public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    PipelineStatus status = getState().getStatus();
    if (status.isActive()) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0118, name);
    }
    OffsetFileUtil.saveSourceOffset(runtimeInfo, name, rev, sourceOffset);
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev);
  }

  @Override
  public void stop(String user) throws PipelineException {
    stopPipeline(false);
  }

  @Override
  public void forceQuit(String user) throws PipelineException {
    if (pipelineRunnable != null && FORCE_QUIT_ALLOWED_STATES.contains(getState().getStatus())) {
      LOG.debug("Force Quit the pipeline '{}'::'{}'", name,  rev);
      pipelineRunnable.forceQuit();
    } else {
      LOG.info("Ignoring force quit request because pipeline is in {} state", getState().getStatus());
    }
  }

  @Override
  public Object getMetrics() throws PipelineStoreException {
    if (prodPipeline != null) {
      Object metrics = prodPipeline.getPipeline().getRunner().getMetrics();
      if (metrics == null && getState().getStatus().isActive()) {
        return metricsForRetry;
      } else {
        return metrics;
      }
    }
    return null;

  }

  @Override
  public String captureSnapshot(
      String user,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException {
    if(batchSize <= 0) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0107, batchSize);
    }
    return captureSnapshot(user, snapshotName, snapshotLabel, batches, batchSize, true);
  }

  public String captureSnapshot(
      String user,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize,
      boolean checkState
  ) throws PipelineException {
    int maxBatchSize = configuration.get(Constants.SNAPSHOT_MAX_BATCH_SIZE_KEY, Constants.SNAPSHOT_MAX_BATCH_SIZE_DEFAULT);

    if(batchSize > maxBatchSize) {
      batchSize = maxBatchSize;
    }

    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    if (checkState) {
      checkState(getState().getStatus().equals(PipelineStatus.RUNNING), ContainerError.CONTAINER_0105);
    }
    SnapshotInfo snapshotInfo = snapshotStore.create(user, name, rev, snapshotName, snapshotLabel);
    prodPipeline.captureSnapshot(snapshotName, batchSize, batches);
    return snapshotInfo.getId();
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel)
      throws PipelineException {
    SnapshotInfo snapshotInfo = snapshotStore.updateLabel(name, rev, snapshotName, snapshotLabel);
    if(snapshotInfo != null) {
      return snapshotInfo.getId();
    }

    return null;
  }

  @Override
  public Snapshot getSnapshot(String id) throws PipelineException {
    return snapshotStore.get(name, rev, id);
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
    return snapshotStore.getSummaryForPipeline(name, rev);
  }

  @Override
  public void deleteSnapshot(String id) throws PipelineException {
    Snapshot snapshot = getSnapshot(id);
    if(snapshot != null && snapshot.getInfo() != null && snapshot.getInfo().isInProgress()) {
      prodPipeline.cancelSnapshot(snapshot.getInfo().getId());
    }
    snapshotStore.deleteSnapshot(name, rev, id);
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return pipelineStateStore.getHistory(name, rev, false);
  }

  @Override
  public void deleteHistory() {
    pipelineStateStore.deleteHistory(name, rev);
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().getStatus().isActive(), ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorRecords(stage, max);
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().getStatus().isActive(), ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorMessages(stage, max);
  }

  @Override
  public List<SampledRecord> getSampledRecords(String sampleId, int max) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().getStatus().isActive(), ContainerError.CONTAINER_0106);
    return observerRunnable.getSampledRecords(sampleId, max);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<AlertInfo> getAlerts() throws PipelineException {
    List<AlertInfo> alertInfoList = new ArrayList<>();
    MetricRegistry metrics = (MetricRegistry)getMetrics();

    if(metrics != null) {
      RuleDefinitions ruleDefinitions = pipelineStoreTask.retrieveRules(name, rev);

      for(RuleDefinition ruleDefinition: ruleDefinitions.getMetricsRuleDefinitions()) {
        Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
          AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

        if(gauge != null) {
          alertInfoList.add(new AlertInfo(name, ruleDefinition, gauge));
        }
      }

      for(RuleDefinition ruleDefinition: ruleDefinitions.getAllDataRuleDefinitions()) {
        Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
          AlertsUtil.getAlertGaugeName(ruleDefinition.getId()));

        if(gauge != null) {
          alertInfoList.add(new AlertInfo(name, ruleDefinition, gauge));
        }
      }
    }

    return alertInfoList;
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().getStatus().isActive(), ContainerError.CONTAINER_0402);
    MetricsConfigurator.resetCounter((MetricRegistry) getMetrics(), AlertsUtil.getUserMetricName(alertId));
    return MetricsConfigurator.removeGauge((MetricRegistry) getMetrics(), AlertsUtil.getAlertGaugeName(alertId), name, rev);
  }

  @Override
  public void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
    throws PipelineRuntimeException {
    try {
      Map<String, Object> allAttributes = null;
      if (attributes != null) {
        allAttributes = new HashMap<>();
        allAttributes.putAll(getState().getAttributes());
        allAttributes.putAll(attributes);
      }
      validateAndSetStateTransition(getState().getUser(), pipelineStatus, message, allAttributes);
    } catch (PipelineStoreException | PipelineRunnerException ex) {
      throw new PipelineRuntimeException(ex.getErrorCode(), ex);
    }
  }

  private void validateAndSetStateTransition(String user, PipelineStatus toStatus, String message, Map<String, Object> attributes)
    throws PipelineStoreException, PipelineRunnerException {
    PipelineState fromState;
    PipelineState pipelineState;
    synchronized (this) {
      fromState = getState();
      checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(toStatus), ContainerError.CONTAINER_0102,
        fromState.getStatus(), toStatus);
      long nextRetryTimeStamp = fromState.getNextRetryTimeStamp();
      int retryAttempt = fromState.getRetryAttempt();
      String metricString = null;
      if (toStatus == PipelineStatus.RETRY && fromState.getStatus() != PipelineStatus.CONNECTING) {
        retryAttempt = fromState.getRetryAttempt() + 1;
        if (retryAttempt > maxRetries && maxRetries != -1) {
          LOG.info("Retry attempt '{}' is greater than max no of retries '{}'", retryAttempt, maxRetries);
          toStatus = PipelineStatus.RUN_ERROR;
          retryAttempt = 0;
          nextRetryTimeStamp = 0;
        } else {
          nextRetryTimeStamp = RetryUtils.getNextRetryTimeStamp(retryAttempt, System.currentTimeMillis());
          isRetrying = true;
          metricsForRetry = getState().getMetrics();
        }
      } else if (!toStatus.isActive()) {
        retryAttempt = 0;
        nextRetryTimeStamp = 0;
      }
      if (!toStatus.isActive() || toStatus == PipelineStatus.DISCONNECTED
        || (toStatus == PipelineStatus.RETRY && fromState.getStatus() != PipelineStatus.CONNECTING)) {
        Object metrics = getMetrics();
        if (metrics != null) {
          ObjectMapper objectMapper = ObjectMapperFactory.get();
          try {
            metricString = objectMapper.writeValueAsString(metrics);
          } catch (JsonProcessingException e) {
            throw new PipelineStoreException(ContainerError.CONTAINER_0210, e.toString(), e);
          }
          eventListenerManager.broadcastMetrics(name, metricString);
        }
        if (metricString == null) {
          metricString = getState().getMetrics();
        }
      }
      pipelineState =
        pipelineStateStore.saveState(user, name, rev, toStatus, message, attributes, ExecutionMode.STANDALONE,
          metricString, retryAttempt, nextRetryTimeStamp);
      if (toStatus == PipelineStatus.RETRY) {
        retryFuture = scheduleForRetries(user, runnerExecutor);
      }
    }
    eventListenerManager.broadcastStateChange(
        fromState,
        pipelineState,
        ThreadUsage.STANDALONE,
        OffsetFileUtil.getOffsets(runtimeInfo, name, rev)
    );
  }



  private void checkState(boolean expr, ContainerError error, Object... args) throws PipelineRunnerException {
    if (!expr) {
      throw new PipelineRunnerException(error, args);
    }
  }

  public static MemoryLimitConfiguration getMemoryLimitConfiguration(PipelineConfigBean pipelineConfiguration)
      throws PipelineRuntimeException {
    //Default memory limit configuration
    MemoryLimitConfiguration memoryLimitConfiguration = new MemoryLimitConfiguration();
    MemoryLimitExceeded memoryLimitExceeded = pipelineConfiguration.memoryLimitExceeded;
    long memoryLimit = pipelineConfiguration.memoryLimit;
    if (memoryLimit > JvmEL.jvmMaxMemoryMB() * Constants.MAX_HEAP_MEMORY_LIMIT_CONFIGURATION) {
      throw new PipelineRuntimeException(ValidationError.VALIDATION_0063,
          memoryLimit,
          "above the maximum",
          JvmEL.jvmMaxMemoryMB() * Constants.MAX_HEAP_MEMORY_LIMIT_CONFIGURATION
      );
    }
    if (memoryLimitExceeded != null && memoryLimit > 0) {
      memoryLimitConfiguration = new MemoryLimitConfiguration(memoryLimitExceeded, memoryLimit);
    }
    return memoryLimitConfiguration;
  }

  @Override
  public void prepareForStart(String user) throws PipelineStoreException, PipelineRunnerException {
    PipelineState fromState = getState();
    checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(PipelineStatus.STARTING), ContainerError.CONTAINER_0102,
        fromState.getStatus(), PipelineStatus.STARTING);

    if(!resourceManager.requestRunnerResources(ThreadUsage.STANDALONE)) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0166, name);
    }
    LOG.info("Preparing to start pipeline '{}::{}'", name, rev);
    validateAndSetStateTransition(user, PipelineStatus.STARTING, null, null);
    token = UUID.randomUUID().toString();
  }

  @Override
  public void prepareForStop(String user) throws PipelineStoreException, PipelineRunnerException {
    LOG.info("Preparing to stop pipeline");
    if (getState().getStatus() == PipelineStatus.RETRY) {
      retryFuture.cancel(true);
      validateAndSetStateTransition(user, PipelineStatus.STOPPING, null, null);
      validateAndSetStateTransition(user, PipelineStatus.STOPPED, "Stopped while the pipeline was in RETRY state", null);
    } else {
      validateAndSetStateTransition(user, PipelineStatus.STOPPING, null, null);
    }
  }

  @Override
  public void start(String user, Map<String, Object> runtimeParameters) throws PipelineException, StageException {
    startPipeline(user, runtimeParameters);
    LOG.debug("Starting the runnable for pipeline {} {}", name, rev);
    if(!pipelineRunnable.isStopped()) {
      pipelineRunnable.run();
    }
  }

  private void startPipeline(String user, Map<String, Object> runtimeParameters) throws PipelineException, StageException {
    Utils.checkState(!isClosed,
        Utils.formatL("Cannot start the pipeline '{}::{}' as the runner is already closed", name, rev));

    synchronized (this) {
      try {
        LOG.info("Starting pipeline {} {}", name, rev);
        UserContext runningUser = new UserContext(user);
      /*
       * Implementation Notes: --------------------- What are the different threads and runnables created? - - - - - - - -
       * - - - - - - - - - - - - - - - - - - - RulesConfigLoader ProductionObserver MetricObserver
       * ProductionPipelineRunner How do threads communicate? - - - - - - - - - - - - - - RulesConfigLoader,
       * ProductionObserver and ProductionPipelineRunner share a blocking queue which will hold record samples to be
       * evaluated by the Observer [Data Rule evaluation] and rules configuration change requests computed by the
       * RulesConfigLoader. MetricsObserverRunner handles evaluating metric rules and a reference is passed to Production
       * Observer which updates the MetricsObserverRunner when configuration changes. Other classes: - - - - - - - - Alert
       * Manager - responsible for creating alerts and sending email.
       */

        PipelineConfiguration pipelineConfiguration = getPipelineConf(name, rev);
        List<Issue> errors = new ArrayList<>();
        PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(
            pipelineConfiguration,
            errors,
            runtimeParameters
        );
        if (pipelineConfigBean == null) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0116, errors);
        }
        maxRetries = pipelineConfigBean.retryAttempts;
        this.runtimeParameters = runtimeParameters;

        MemoryLimitConfiguration memoryLimitConfiguration = getMemoryLimitConfiguration(pipelineConfigBean);

        BlockingQueue<Object> productionObserveRequests =
            new ArrayBlockingQueue<>(configuration.get(Constants.OBSERVER_QUEUE_SIZE_KEY,
                Constants.OBSERVER_QUEUE_SIZE_DEFAULT), true /* FIFO */);

        BlockingQueue<Record> statsQueue = null;
        boolean statsAggregationEnabled = isStatsAggregationEnabled(pipelineConfiguration);
        if (statsAggregationEnabled) {
          statsQueue = new ArrayBlockingQueue<>(
              configuration.get(
                  Constants.STATS_AGGREGATOR_QUEUE_SIZE_KEY,
                  Constants.STATS_AGGREGATOR_QUEUE_SIZE_DEFAULT
              ),
              true /* FIFO */
          );
        }

        //Need to augment the existing object graph with pipeline related modules.
        //This ensures that the singletons defined in those modules are singletons within the
        //scope of a pipeline.
        //So if a pipeline is started again for the second time, the object graph recreates the production pipeline
        //with fresh instances of MetricRegistry, alert manager, observer etc etc..
        ObjectGraph objectGraph = this.objectGraph.plus(
            new PipelineProviderModule(
                name,
                pipelineConfiguration.getTitle(),
                rev,
                statsAggregationEnabled,
                pipelineConfigBean.constants
            )
        );

        threadHealthReporter = objectGraph.get(ThreadHealthReporter.class);
        observerRunnable = objectGraph.get(DataObserverRunnable.class);
        metricsEventRunnable = objectGraph.get(MetricsEventRunnable.class);

        ProductionObserver productionObserver = (ProductionObserver) objectGraph.get(Observer.class);
        RulesConfigLoader rulesConfigLoader = objectGraph.get(RulesConfigLoader.class);
        RulesConfigLoaderRunnable rulesConfigLoaderRunnable = objectGraph.get(RulesConfigLoaderRunnable.class);
        MetricObserverRunnable metricObserverRunnable = objectGraph.get(MetricObserverRunnable.class);
        ProductionPipelineRunner runner = (ProductionPipelineRunner) objectGraph.get(PipelineRunner.class);
        runner.addErrorListeners(errorListeners);
        if (isRetrying) {
          ObjectMapper objectMapper = ObjectMapperFactory.get();
          MetricRegistryJson metricRegistryJson = null;
          try {
            if (metricsForRetry != null) {
              metricRegistryJson = objectMapper.readValue(metricsForRetry, MetricRegistryJson.class);
              runner.updateMetrics(metricRegistryJson);
              observerRunnable.setMetricRegistryJson(metricRegistryJson);
            }
          } catch (IOException ex) {
            LOG.warn("Error while serializing slave metrics: , {}", ex.toString(), ex);
          }
          isRetrying = false;
        }
        if (pipelineConfigBean.rateLimit > 0) {
          runner.setRateLimit(pipelineConfigBean.rateLimit);
        }
        ProductionPipelineBuilder builder = objectGraph.get(ProductionPipelineBuilder.class);

        //register email notifier & webhook notifier with event listener manager
        registerEmailNotifierIfRequired(pipelineConfigBean, name, pipelineConfiguration.getTitle(),rev);
        registerWebhookNotifierIfRequired(pipelineConfigBean, name, pipelineConfiguration.getTitle(), rev);

        //This which are not injected as of now.
        productionObserver.setObserveRequests(productionObserveRequests);
        runner.setObserveRequests(productionObserveRequests);
        runner.setStatsAggregatorRequests(statsQueue);
        runner.setDeliveryGuarantee(pipelineConfigBean.deliveryGuarantee);
        runner.setMemoryLimitConfiguration(memoryLimitConfiguration);

        PipelineEL.setConstantsInContext(pipelineConfiguration, runningUser);
        prodPipeline = builder.build(
          runningUser,
          pipelineConfiguration,
          getState().getTimeStamp(),
          runtimeParameters
        );
        prodPipeline.registerStatusListener(this);

        ScheduledFuture<?> metricsFuture = null;
        metricsEventRunnable.setStatsQueue(statsQueue);
        metricsEventRunnable.setPipelineConfiguration(pipelineConfiguration);
        int refreshInterval = configuration.get(MetricsEventRunnable.REFRESH_INTERVAL_PROPERTY,
            MetricsEventRunnable.REFRESH_INTERVAL_PROPERTY_DEFAULT);
        if(refreshInterval > 0) {
          metricsFuture =
              runnerExecutor.scheduleAtFixedRate(metricsEventRunnable, 0, metricsEventRunnable.getScheduledDelay(),
                  TimeUnit.MILLISECONDS);
        }
        //Schedule Rules Config Loader
        rulesConfigLoader.setStatsQueue(statsQueue);
        try {
          rulesConfigLoader.load(productionObserver);
        } catch (InterruptedException e) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0403, name, e.toString(), e);
        }
        ScheduledFuture<?> configLoaderFuture =
            runnerExecutor.scheduleWithFixedDelay(rulesConfigLoaderRunnable, 1, RulesConfigLoaderRunnable.SCHEDULED_DELAY,
                TimeUnit.SECONDS);

        ScheduledFuture<?> metricObserverFuture = runnerExecutor.scheduleWithFixedDelay(metricObserverRunnable, 1, 2,
            TimeUnit.SECONDS);

        // update checker
        updateChecker = new UpdateChecker(runtimeInfo, configuration, pipelineConfiguration, this);
        ScheduledFuture<?> updateCheckerFuture = runnerExecutor.scheduleAtFixedRate(updateChecker, 1, 24 * 60, TimeUnit.MINUTES);

        observerRunnable.setRequestQueue(productionObserveRequests);
        observerRunnable.setStatsQueue(statsQueue);
        Future<?> observerFuture = runnerExecutor.submit(observerRunnable);

        List<Future<?>> list;
        if (metricsFuture != null) {
          list =
              ImmutableList
                  .of(configLoaderFuture, observerFuture, metricObserverFuture, metricsFuture, updateCheckerFuture);
        } else {
          list = ImmutableList.of(configLoaderFuture, observerFuture, metricObserverFuture, updateCheckerFuture);
        }
        pipelineRunnable = new ProductionPipelineRunnable(threadHealthReporter, this, prodPipeline, name, rev, list);
      } catch (Exception e) {
        validateAndSetStateTransition(user, PipelineStatus.START_ERROR, e.toString(), null);
        throw e;
      }
    }
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
    startPipeline(user, runtimeParameters);
    captureSnapshot(user, snapshotName, snapshotLabel, batches, batchSize, false);
    LOG.debug("Starting the runnable for pipeline {} {}", name, rev);
    if(!pipelineRunnable.isStopped()) {
      pipelineRunnable.run();
    }
  }

  private boolean isStatsAggregationEnabled(PipelineConfiguration pipelineConfiguration) throws PipelineStoreException {
    boolean isEnabled = false;
    StageConfiguration statsAggregatorStage = pipelineConfiguration.getStatsAggregatorStage();
    if (statsAggregatorStage != null &&
        !statsAggregatorStage.getStageName().equals(STATS_NULL_TARGET) &&
        !statsAggregatorStage.getStageName().equals(STATS_DPM_DIRECTLY_TARGET) &&
        pipelineConfiguration.getMetadata() != null) {
      isEnabled = true;
    }
    return isEnabled && this.isRemotePipeline();
  }

  private void stopPipeline(boolean sdcShutting) throws PipelineException {
    if (pipelineRunnable != null && !pipelineRunnable.isStopped()) {
      LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
      // this is sync call, will wait till pipeline is in terminal state
      pipelineRunnable.stop(sdcShutting);
      pipelineRunnable = null;
    }
    if (metricsEventRunnable != null) {
      metricsEventRunnable.onStopPipeline();
      metricsEventRunnable = null;
    }
    if (threadHealthReporter != null) {
      threadHealthReporter.destroy();
      threadHealthReporter = null;
    }
  }

  @Override
  public void close() {
    isClosed = true;
  }

  @Override
  public Map getUpdateInfo() {
    return updateChecker.getUpdateInfo();
  }

  public Pipeline getPipeline() {
    return prodPipeline != null ? prodPipeline.getPipeline() : null;
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    throw new UnsupportedOperationException("This method is only supported in Cluster Runner");
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    throw new UnsupportedOperationException("This method is only supported in Cluster Runner");
  }

  @Override
  public String getToken() {
    return token;
  }

  @Override
  public int getRunnerCount() {
    return prodPipeline != null ? prodPipeline.getPipeline().getNumOfRunners() : 0;
  }
}
