/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.standalone;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service.State;
import com.streamsets.dc.execution.PipelineState;
import com.streamsets.dc.execution.PipelineStateStore;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.Snapshot;
import com.streamsets.dc.execution.SnapshotInfo;
import com.streamsets.dc.execution.SnapshotStore;
import com.streamsets.dc.execution.StateListener;
import com.streamsets.dc.execution.alerts.AlertManager;
import com.streamsets.dc.execution.metrics.MetricsEventRunnable;
import com.streamsets.dc.execution.runner.common.Constants;
import com.streamsets.dc.execution.runner.common.DataObserverRunnable;
import com.streamsets.dc.execution.runner.common.MetricObserverRunnable;
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.dc.execution.runner.common.ProductionObserver;
import com.streamsets.dc.execution.runner.common.ProductionPipeline;
import com.streamsets.dc.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.dc.execution.runner.common.ProductionPipelineRunnable;
import com.streamsets.dc.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.dc.execution.runner.common.dagger.PipelineProviderModule;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.dc.callback.CallbackInfo;
import com.streamsets.pipeline.callback.CallbackServerMetricsEventListener;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.creation.PipelineBeanCreator;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.pipeline.el.JvmEL;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.runner.production.RulesConfigLoader;
import com.streamsets.pipeline.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.pipeline.runner.production.ThreadHealthReporter;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.validation.Issue;
import com.streamsets.pipeline.validation.ValidationError;
import com.streamsets.pipeline.util.PipelineException;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StandaloneRunner implements Runner, StateListener {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneRunner.class);

  @Inject RuntimeInfo runtimeInfo;
  @Inject Configuration configuration;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject StageLibraryTask stageLibrary;
  @Inject SnapshotStore snapshotStore;
  @Inject PipelineStoreTask pipelineStore;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;

  private final ObjectGraph objectGraph;
  private final String name;
  private final String rev;
  private final String user;

  /*Mutex objects to synchronize start and stop pipeline methods*/
  private AlertManager alertManager;
  private ThreadHealthReporter threadHealthReporter;
  private DataObserverRunnable observerRunnable;
  private ProductionPipeline prodPipeline;
  private final MetricsEventRunnable metricsEventRunnable;
  private ProductionPipelineRunnable pipelineRunnable;
  private volatile boolean isClosed;

  private static final Map<PipelineStatus, Set<PipelineStatus>> VALID_TRANSITIONS =
    new ImmutableMap.Builder<PipelineStatus, Set<PipelineStatus>>()
    .put(PipelineStatus.EDITED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STARTING, ImmutableSet.of(PipelineStatus.START_ERROR, PipelineStatus.RUNNING,
      PipelineStatus.DISCONNECTING, PipelineStatus.STOPPING))
    .put(PipelineStatus.START_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.RUNNING, ImmutableSet.of(PipelineStatus.RUNNING_ERROR, PipelineStatus.FINISHING,
      PipelineStatus.STOPPING, PipelineStatus.DISCONNECTING))
    .put(PipelineStatus.RUNNING_ERROR, ImmutableSet.of(PipelineStatus.RUN_ERROR))
    .put(PipelineStatus.RUN_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.FINISHING, ImmutableSet.of(PipelineStatus.FINISHED))
    .put(PipelineStatus.STOPPING, ImmutableSet.of(PipelineStatus.STOPPED))
    .put(PipelineStatus.FINISHED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.DISCONNECTING, ImmutableSet.of(PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.DISCONNECTED, ImmutableSet.of(PipelineStatus.CONNECTING))
    .put(PipelineStatus.CONNECTING, ImmutableSet.of(PipelineStatus.STARTING, PipelineStatus.DISCONNECTING))
    .build();

  public StandaloneRunner(String user, String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.objectGraph = objectGraph;
    objectGraph.inject(this);
    int refreshInterval = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);
    if (refreshInterval > 0) {
      metricsEventRunnable = new MetricsEventRunnable(runtimeInfo, refreshInterval, this, threadHealthReporter);
    } else {
      metricsEventRunnable = null;
    }

  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline " + name + " with rev " + rev + " is in state: " + status);
    String msg = null;
    switch (status) {
      case STARTING:
        msg = "Pipeline was in STARTING state, forcing it to DISCONNECTING";
      case CONNECTING:
        msg = msg == null ? "Pipeline was in CONNECTING state, forcing it to DISCONNECTING" : msg;
      case RUNNING:
        msg = msg == null ? "Pipeline was in RUNNING state, forcing it to DISCONNECTING" : msg;
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.DISCONNECTING, msg, null);
      case DISCONNECTING:
        msg = "Pipeline was in DISCONNECTING state, forcing it to DISCONNECTED";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.DISCONNECTED, msg, null);
      case DISCONNECTED:
        break;
      case RUNNING_ERROR:
        msg = "Pipeline was in RUNNING_ERROR state, forcing it to terminal state of RUN_ERROR";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.RUN_ERROR, msg, null);
        break;
      case STOPPING:
        msg = "Pipeline was in STOPPING state, forcing it to terminal state of STOPPED";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.STOPPED, msg, null);
        break;
      case FINISHING:
        msg = "Pipeline was in FINISHING state, forcing it to terminal state of FINISHED";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.FINISHED, null, null);
        break;
      case RUN_ERROR: // do nothing
      case EDITED:
      case FINISHED:
      case KILLED:
      case START_ERROR:
      case STOPPED:
        break;
      default:
        throw new IllegalStateException(Utils.format("Pipeline in undefined state: '{}'", status));
    }
  }

  @Override
  public void onDataCollectorStart() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
    switch (status) {
      case DISCONNECTED:
        String msg = "Pipeline was in DISCONNECTED state, changing it to CONNECTING";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.CONNECTING, msg, null);
        prepareForStart();
        start();
      default:
        LOG.error(Utils.format("Pipeline cannot start with status: '{}'", status));
    }
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventRunnable.addMetricsEventListener(metricsEventListener);
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    alertManager.addAlertEventListener(alertEventListener);
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    alertManager.removeAlertEventListener(alertEventListener);
  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    alertManager.broadcastAlerts(ruleDefinition);
  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException {
    if (!getState().getStatus().isActive() || getState().getStatus() == PipelineStatus.DISCONNECTED) {
      LOG.info("Pipeline '{}'::'{}' is no longer active", name, rev);
      return;
    }
    LOG.info("Stopping pipeline {}::{}", name, rev);
    try {
      try {
        validateAndSetStateTransition(PipelineStatus.DISCONNECTING, "Stopping the pipeline as SDC is shutting down",
          null);
      } catch (PipelineRunnerException ex) {
        // its ok if state validation fails - we will still try to stop pipeline if active
        LOG.warn("Cannot transition to PipelineStatus.DISCONNECTING: {}", ex.getMessage(), ex);
      }
      stopPipeline(true /* shutting down node process */);
    } catch (Exception e) {
      LOG.warn("Error while stopping the pipeline: {} ", e.getMessage(), e);
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
  public String getUser() {
    return user;
  }

  @Override
  public synchronized void resetOffset() throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    LOG.debug("Resetting offset for pipeline {}, {}", name, rev);
    if(status == PipelineStatus.RUNNING) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0104, name);
    }
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    offsetTracker.resetOffset(name, rev);
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev);
  }

  @Override
  public synchronized void stop() throws PipelineStoreException, PipelineRunnerException {
    validateAndSetStateTransition(PipelineStatus.STOPPING, "Stopping the pipeline", null);
    stopPipeline(false);
  }

  @Override
  public Object getMetrics() {
    if (prodPipeline != null) {
      return prodPipeline.getPipeline().getRunner().getMetrics();
    }
    return null;
  }

  @Override
  public String captureSnapshot(String snapshotName, int batches, int batchSize)
    throws PipelineException {
    int maxBatchSize = configuration.get(Constants.SNAPSHOT_MAX_BATCH_SIZE_KEY, Constants.SNAPSHOT_MAX_BATCH_SIZE_DEFAULT);

    if(batchSize > maxBatchSize) {
      batchSize = maxBatchSize;
    }

    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    checkState(getState().getStatus().equals(PipelineStatus.RUNNING), ContainerError.CONTAINER_0105);
    if(batchSize <= 0) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0107, batchSize);
    }
    SnapshotInfo snapshotInfo = snapshotStore.create(user, name, rev, snapshotName);
    prodPipeline.captureSnapshot(snapshotName, batchSize, batches);
    return snapshotInfo.getId();
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
    snapshotStore.deleteSnapshot(name, rev, id);
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
  public List<Record> getSampledRecords(String sampleId, int max) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().getStatus().isActive(), ContainerError.CONTAINER_0106);
    return observerRunnable.getSampledRecords(sampleId, max);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    checkState(getState().equals(PipelineStatus.RUNNING), ContainerError.CONTAINER_0402);
    MetricsConfigurator.resetCounter((MetricRegistry)getMetrics(), AlertsUtil.getUserMetricName(alertId));
    return MetricsConfigurator.removeGauge((MetricRegistry)getMetrics(), AlertsUtil.getAlertGaugeName(alertId), name ,rev);
  }

  @Override
  public void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
    throws PipelineRuntimeException {
    try {
      validateAndSetStateTransition(pipelineStatus, message, attributes);
    } catch (PipelineStoreException | PipelineRunnerException ex) {
      throw new PipelineRuntimeException(ex.getErrorCode(), ex);
    }
  }

  private synchronized void validateAndSetStateTransition(PipelineStatus toStatus, String message, Map<String, Object> attributes)
    throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    checkState(VALID_TRANSITIONS.get(status).contains(toStatus), ContainerError.CONTAINER_0102, status, toStatus);
    pipelineStateStore.saveState(user, name, rev, toStatus, message, attributes, ExecutionMode.STANDALONE);
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
    if (memoryLimit > JvmEL.jvmMaxMemoryMB() * 0.85) {
      throw new PipelineRuntimeException(ValidationError.VALIDATION_0063, memoryLimit,
                                         "above the maximum", JvmEL.jvmMaxMemoryMB() * 0.85);
    }
    if (memoryLimitExceeded != null && memoryLimit > 0) {
      memoryLimitConfiguration = new MemoryLimitConfiguration(memoryLimitExceeded, memoryLimit);
    }
    return memoryLimitConfiguration;
  }

  @Override
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException {
      LOG.info("Preparing to start pipeline '{}::{}'", name, rev);
      validateAndSetStateTransition(PipelineStatus.STARTING, null, null);
  }

  @Override
  public void start() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
    Utils.checkState(!isClosed,
      Utils.formatL("Cannot start the pipeline '{}::{}' as the runner is already closed", name, rev));

    synchronized (this) {
      LOG.info("Starting pipeline {} {}", name, rev);
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

      PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);
      List<Issue> errors = new ArrayList<>();
      PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipelineConfiguration, errors);
      if (pipelineConfigBean == null) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0116, errors);
      }

      MemoryLimitConfiguration memoryLimitConfiguration = getMemoryLimitConfiguration(pipelineConfigBean);

      BlockingQueue<Object> productionObserveRequests =
        new ArrayBlockingQueue<>(configuration.get(Constants.OBSERVER_QUEUE_SIZE_KEY,
          Constants.OBSERVER_QUEUE_SIZE_DEFAULT), true /* FIFO */);

      //Need to augment the existing object graph with pipeline related modules.
      //This ensures that the singletons defined in those modules are singletons within the
      //scope of a pipeline.
      //So if a pipeline is started again for the second time, the object graph recreates the production pipeline
      //with fresh instances of MetricRegistry, alert manager, observer etc etc..
      ObjectGraph objectGraph = this.objectGraph.plus(new PipelineProviderModule(name, rev));

      alertManager = objectGraph.get(AlertManager.class);
      threadHealthReporter = objectGraph.get(ThreadHealthReporter.class);
      observerRunnable = objectGraph.get(DataObserverRunnable.class);

      ProductionObserver productionObserver = (ProductionObserver)objectGraph.get(Observer.class);
      RulesConfigLoader rulesConfigLoader = objectGraph.get(RulesConfigLoader.class);
      RulesConfigLoaderRunnable rulesConfigLoaderRunnable = objectGraph.get(RulesConfigLoaderRunnable.class);
      MetricObserverRunnable metricObserverRunnable = objectGraph.get(MetricObserverRunnable.class);
      ProductionPipelineRunner runner = (ProductionPipelineRunner)objectGraph.get(PipelineRunner.class);
      ProductionPipelineBuilder builder = objectGraph.get(ProductionPipelineBuilder.class);

      //This which are not injected as of now.
      productionObserver.setObserveRequests(productionObserveRequests);
      runner.setObserveRequests(productionObserveRequests);
      runner.setDeliveryGuarantee(pipelineConfigBean.deliveryGuarantee);
      runner.setMemoryLimitConfiguration(memoryLimitConfiguration);

      prodPipeline = builder.build(pipelineConfiguration);
      prodPipeline.registerStatusListener(this);

      ScheduledFuture<?> metricsFuture = null;
      if (metricsEventRunnable != null) {
        metricsFuture =
          runnerExecutor.scheduleAtFixedRate(metricsEventRunnable, 0, metricsEventRunnable.getScheduledDelay(),
            TimeUnit.MILLISECONDS);
      }
      //Schedule Rules Config Loader
      try {
        rulesConfigLoader.load(productionObserver);
      } catch (InterruptedException e) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0403, name, e.getMessage(), e);
      }
      ScheduledFuture<?> configLoaderFuture =
        runnerExecutor.scheduleWithFixedDelay(rulesConfigLoaderRunnable, 1, RulesConfigLoaderRunnable.SCHEDULED_DELAY,
          TimeUnit.SECONDS);

      ScheduledFuture<?> metricObserverFuture = runnerExecutor.scheduleWithFixedDelay(metricObserverRunnable, 1, 2,
        TimeUnit.SECONDS);

      observerRunnable.setRequestQueue(productionObserveRequests);
      Future<?> observerFuture = runnerExecutor.submit(observerRunnable);

      List<Future<?>> list;
      if (metricsFuture != null) {
        list = ImmutableList.of(configLoaderFuture, observerFuture, metricObserverFuture, metricsFuture);
      } else {
        list = ImmutableList.of(configLoaderFuture, observerFuture, metricObserverFuture);
      }
      pipelineRunnable = new ProductionPipelineRunnable(threadHealthReporter, this, prodPipeline, name, rev, list);
    }

    if(!pipelineRunnable.isStopped()) {
      pipelineRunnable.run();
    }
    LOG.debug("Started pipeline {} {}", name, rev);
  }

  private synchronized void stopPipeline(boolean sdcShutting) {
   if (pipelineRunnable != null && !pipelineRunnable.isStopped()) {
      LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
      pipelineRunnable.stop(sdcShutting);
    }
    if(metricsEventRunnable != null) {
      metricsEventRunnable.setThreadHealthReporter(null);
    }
    if (threadHealthReporter != null) {
      threadHealthReporter.destroy();
      threadHealthReporter = null;
    }
    LOG.debug("Stopped pipeline");
  }

  @Override
  public void close() {
    isClosed = true;
  }

  public Pipeline getPipeline() {
    return prodPipeline != null ? prodPipeline.getPipeline() : null;
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    throw new UnsupportedOperationException("This method is only supported in Cluster Runner");
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    throw new UnsupportedOperationException("This method is only supported in Cluster Runner");
  }
}
