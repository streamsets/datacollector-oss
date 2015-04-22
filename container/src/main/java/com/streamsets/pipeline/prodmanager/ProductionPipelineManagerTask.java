/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.email.EmailSender;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.metrics.MetricsEventRunnable;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.DataObserverRunnable;
import com.streamsets.pipeline.runner.production.MetricObserverRunnable;
import com.streamsets.pipeline.runner.production.MetricsObserverRunner;
import com.streamsets.pipeline.runner.production.ProductionObserver;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.runner.production.ProductionPipelineBuilder;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunnable;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunner;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.runner.production.RulesConfigLoader;
import com.streamsets.pipeline.runner.production.RulesConfigLoaderRunnable;
import com.streamsets.pipeline.runner.production.ThreadHealthReporter;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.util.ValidationUtil;
import com.streamsets.pipeline.validation.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ProductionPipelineManagerTask extends AbstractTask {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineManagerTask.class);
  private static final String PRODUCTION_PIPELINE_MANAGER = "productionPipelineManager";
  private static final String PRODUCTION_PIPELINE_RUNNER = "ProductionPipelineRunner";
  private static final String RUN_INFO_DIR = "runInfo";

  private static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  private static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;

  private static final Map<State, Set<State>> VALID_TRANSITIONS = new ImmutableMap.Builder<State, Set<State>>()
    .put(State.STOPPED, ImmutableSet.of(State.RUNNING))
    .put(State.FINISHED, ImmutableSet.of(State.RUNNING))
    .put(State.RUNNING, ImmutableSet.of(State.STOPPING, State.FINISHED))
    .put(State.STOPPING, ImmutableSet.of(State.STOPPING /*Try stopping many times, this should be no-op*/
        , State.STOPPED, State.NODE_PROCESS_SHUTDOWN))
    .put(State.ERROR, ImmutableSet.of(State.RUNNING, State.STOPPED))
    .put(State.NODE_PROCESS_SHUTDOWN, ImmutableSet.of(State.RUNNING))
    .build();

  private final RuntimeInfo runtimeInfo;
  private final StateTracker stateTracker;
  private final com.streamsets.pipeline.util.Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final StageLibraryTask stageLibrary;
  private final SnapshotStore snapshotStore;
  private ThreadHealthReporter threadHealthReporter;

  /*References the thread that is executing the pipeline currently */
  private ProductionPipelineRunnable pipelineRunnable;
  /*References the thread that is executing the observer stuff*/
  private DataObserverRunnable observerRunnable;
  private MetricObserverRunnable metricObserverRunnable;
  /*Thread that is watching changes to the rules configuration*/
  private RulesConfigLoaderRunnable configLoaderRunnable;
  /*The executor service that is currently executing the ProdPipelineRunnerThread*/
  private SafeScheduledExecutorService executor;
  /*The pipeline being executed or the pipeline in the context*/
  private ProductionPipeline prodPipeline;
  /*References the thread that is serializing the metrics object */
  private MetricsEventRunnable metricsEventRunnable;

  /*Mutex objects to synchronize start and stop pipeline methods*/
  private final Object pipelineMutex = new Object();

  private List<AlertEventListener> alertEventListenerList = new ArrayList<>();


  @Inject
  public ProductionPipelineManagerTask(RuntimeInfo runtimeInfo,
      com.streamsets.pipeline.util.Configuration configuration, PipelineStoreTask pipelineStore,
      StageLibraryTask stageLibrary) {
    super(PRODUCTION_PIPELINE_MANAGER);
    this.runtimeInfo = runtimeInfo;
    stateTracker = new StateTracker(runtimeInfo, configuration);
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    snapshotStore = new FileSnapshotStore(runtimeInfo);
  }


  public PipelineState getPipelineState() {
    return stateTracker.getState();
  }

  public void setState(String name, String rev, State state, String message, MetricRegistry metricRegistry) throws PipelineManagerException {
    stateTracker.setState(name, rev, state, message, metricRegistry);
  }

  public void addStateEventListener(StateEventListener stateListener) {
    stateTracker.addStateEventListener(stateListener);
  }

  public void removeStateEventListener(StateEventListener stateListener) {
    stateTracker.removeStateEventListener(stateListener);
  }

  public void addAlertEventListener(AlertEventListener alertEventListener) {
    alertEventListenerList.add(alertEventListener);
  }

  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    alertEventListenerList.remove(alertEventListener);
  }

  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventRunnable.addMetricsEventListener(metricsEventListener);
  }

  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventRunnable.removeMetricsEventListener(metricsEventListener);
  }

  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    if(alertEventListenerList.size() > 0) {
      try {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String ruleDefinitionJSONStr = objectMapper.writer().writeValueAsString(ruleDefinition);
        for(AlertEventListener alertEventListener : alertEventListenerList) {
          try {
            alertEventListener.notification(ruleDefinitionJSONStr);
          } catch (Exception ex) {
            LOG.warn("Error while notifying alerts, {}", ex.getMessage(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting alerts, {}", ex.getMessage(), ex);
      }
    }
  }

  @Override
  public void initTask() {
    LOG.debug("Initializing Production Pipeline Manager");
    stateTracker.init();
    executor = new SafeScheduledExecutorService(4, PRODUCTION_PIPELINE_RUNNER);
    PipelineState ps = getPipelineState();
    if(ps != null) {
      switch (ps.getState()) {
        case RUNNING:
          //Restart after a non orderly shutdown [like kill -9]
          restartPipeline(ps);
          break;
        case NODE_PROCESS_SHUTDOWN:
          //Restart after an orderly shutdown [like Ctrl - C]
          try {
            LOG.debug("Starting pipeline {} {}", ps.getName(), ps.getRev());
            //Start pipeline changes state from NODE_PROCESS_SHUTDOWN to RUNNING where as handleStartRequest does not.
            //We need to change state here
            startPipeline(ps.getName(), ps.getRev());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          //Normal start/restart
          //create the pipeline instance. This was the pipeline in the context before the manager shutdown previously
          try {
            BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(
              configuration.get(Configuration.OBSERVER_QUEUE_SIZE_KEY, Configuration.OBSERVER_QUEUE_SIZE_DEFAULT),
              true /*FIFO*/);
            ProductionObserver observer = new ProductionObserver(productionObserveRequests, configuration);
            createPipeline(ps.getName(), ps.getRev(), observer, productionObserveRequests);
            AlertManager alertManager = new AlertManager(ps.getName(), ps.getRev(), new EmailSender(configuration),
              getMetrics(), runtimeInfo, this);
            MetricsObserverRunner metricsObserverRunner = new MetricsObserverRunner(this.getMetrics(), alertManager);
            observer.setMetricsObserverRunner(metricsObserverRunner);
          } catch (Exception e) {
            //log error and shutdown again
            LOG.error(ContainerError.CONTAINER_0108.getMessage(), e.getMessage(), e);
          }
      }
    }

    long refreshInterval = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);

    if(refreshInterval > 0) {
      metricsEventRunnable = new MetricsEventRunnable(this);
      executor.scheduleAtFixedRate(metricsEventRunnable, 0, refreshInterval, TimeUnit.MILLISECONDS);
    }

    LOG.debug("Initialized Production Pipeline Manager");
  }

  private void restartPipeline(PipelineState ps) {
    try {
      LOG.debug("Starting pipeline {} {}", ps.getName(), ps.getRev());
      //Note that the state is already "RUNNING" in this case. No need to change state.
      handleStartRequest(ps.getName(), ps.getRev());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stopTask() {
    LOG.debug("Stopping Production Pipeline Manager");
    PipelineState ps = getPipelineState();
    if(ps != null) {
      if (State.RUNNING.equals(ps.getState())) {
        LOG.debug("Stopping pipeline {} {}", ps.getName(), ps.getRev());
        try {
          stopPipeline(true /*shutting down node process*/);
        } catch (PipelineManagerException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if(executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        executor.shutdownNow();
        LOG.warn(Utils.format("Forced termination. Reason {}", e.getMessage()));
      }
    }
    LOG.debug("Stopped Production Pipeline Manager");
  }

  public void resetOffset(String pipelineName, String rev) throws PipelineManagerException {
    PipelineState pState = getPipelineState();
    if(pState == null) {
      return;
    }
    LOG.debug("Resetting offset for pipeline {}", pipelineName);
    if(pState.getName().equals(pipelineName) && pState.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0104, pipelineName);
    }
    createPipelineDirIfNotExist(pipelineName);
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(pipelineName, rev, runtimeInfo);
    offsetTracker.resetOffset(pipelineName, rev);
  }

  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineStoreException{
    List<SnapshotInfo> snapshotInfos = new ArrayList<>();
    for(PipelineInfo pipelineInfo: pipelineStore.getPipelines()) {
      snapshotInfos.addAll(snapshotStore.getSnapshotsInfo(pipelineInfo.getName(), pipelineInfo.getLastRev()));
    }
    return snapshotInfos;
  }

  public void captureSnapshot(String snapshotName, int batchSize) throws PipelineManagerException {
    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    checkState(getPipelineState() != null && getPipelineState().getState().equals(State.RUNNING),
      ContainerError.CONTAINER_0105);
    if(batchSize <= 0) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0107, batchSize);
    }
    prodPipeline.captureSnapshot(snapshotName, batchSize);
    LOG.debug("Captured snapshot with batch size {}", batchSize);
  }

  public SnapshotStatus getSnapshotStatus(String snapshotName) {
    return snapshotStore.getSnapshotStatus(stateTracker.getState().getName(), stateTracker.getState().getRev(),
      snapshotName);
  }

  public InputStream getSnapshot(String pipelineName, String rev, String snapshotName) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return snapshotStore.getSnapshot(pipelineName, rev, snapshotName);
  }

  public List<Record> getErrorRecords(String instanceName, int size) throws PipelineManagerException {
    checkState(getPipelineState().getState().equals(State.RUNNING), ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorRecords(instanceName, size);
  }

  public List<Record> getSampledRecords(String sampleDefinitionId, int size) throws PipelineManagerException {
    checkState(getPipelineState().getState().equals(State.RUNNING), ContainerError.CONTAINER_0106);
    return observerRunnable.getSampledRecords(sampleDefinitionId, size);
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) throws PipelineManagerException {
    checkState(getPipelineState() != null && getPipelineState().getState().equals(State.RUNNING),
      ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorMessages(instanceName, size);
  }

  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning)
    throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return stateTracker.getHistory(pipelineName, rev, fromBeginning);
  }

  public void deleteSnapshot(String pipelineName, String rev, String snapshotName) {
    LOG.debug("Deleting snapshot");
    snapshotStore.deleteSnapshot(pipelineName, rev, snapshotName);
    LOG.debug("Deleted snapshot");
  }

  public PipelineState startPipeline(String name, String rev) throws PipelineStoreException
      , PipelineManagerException, PipelineRuntimeException, StageException {
    synchronized (pipelineMutex) {
      LOG.info("Starting pipeline {} {}", name, rev);
      validateStateTransition(name, rev, State.RUNNING);
      handleStartRequest(name, rev);
      setState(name, rev, State.RUNNING, null, null);
      return getPipelineState();
    }
  }

  public PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException {
    synchronized (pipelineMutex) {
      validateStateTransition(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING);
      setState(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING,
        Configuration.STOP_PIPELINE_MESSAGE, getMetrics());
      PipelineState pipelineState = getPipelineState();
      handleStopRequest(nodeProcessShutdown);
      return pipelineState;
    }
  }

  public MetricRegistry getMetrics() {
    if(prodPipeline != null) {
      return prodPipeline.getPipeline().getRunner().getMetrics();
    }

    return null;
  }

  private void handleStartRequest(String name, String rev) throws PipelineManagerException, StageException
      , PipelineRuntimeException, PipelineStoreException {

  /*
    Implementation Notes:
    ---------------------

    What are the different threads and runnables created?
    - - - - - - - - - - - - - - - - - - - - - - - - - - -
     RulesConfigLoader
     ProductionObserver
     MetricObserver
     ProductionPipelineRunner

    How do threads communicate?
    - - - - - - - - - - - - - -

     RulesConfigLoader, ProductionObserver and ProductionPipelineRunner share a blocking queue which will hold
     record samples to be evaluated by the Observer [Data Rule evaluation] and rules configuration change requests
     computed by the RulesConfigLoader.

     MetricsObserverRunner handles evaluating metric rules and a reference is passed to Production Observer which
     updates the MetricsObserverRunner when configuration changes.

    Other classes:
    - - - - - - - -

    Alert Manager - responsible for creating alerts and sendign email.


  */

    BlockingQueue<Object> productionObserveRequests = new ArrayBlockingQueue<>(
      configuration.get(Configuration.OBSERVER_QUEUE_SIZE_KEY, Configuration.OBSERVER_QUEUE_SIZE_DEFAULT),
      true /*FIFO*/);

    ProductionObserver observer = new ProductionObserver(productionObserveRequests, configuration);
    createPipeline(name, rev, observer, productionObserveRequests);

    AlertManager alertManager = new AlertManager(name, rev, new EmailSender(configuration), getMetrics(), runtimeInfo, this);
    MetricsObserverRunner metricsObserverRunner = new MetricsObserverRunner(this.getMetrics(), alertManager);

    observer.setMetricsObserverRunner(metricsObserverRunner);

    //bootstrap the pipeline runner thread with rule configuration
    RulesConfigLoader rulesConfigLoader = new RulesConfigLoader(name, rev, pipelineStore);
    try {
      rulesConfigLoader.load(observer);
    } catch (InterruptedException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0403, name, e.getMessage(), e);
    }

    threadHealthReporter = new ThreadHealthReporter(this.getMetrics());
    threadHealthReporter.register(RulesConfigLoaderRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(MetricObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(DataObserverRunnable.RUNNABLE_NAME);
    threadHealthReporter.register(ProductionPipelineRunnable.RUNNABLE_NAME);

    configLoaderRunnable = new RulesConfigLoaderRunnable(threadHealthReporter, rulesConfigLoader, observer);
    ScheduledFuture<?> configLoaderFuture =
      executor.scheduleWithFixedDelayReturnFuture(configLoaderRunnable, 1,
        RulesConfigLoaderRunnable.SCHEDULED_DELAY, TimeUnit.SECONDS);

    metricObserverRunnable = new MetricObserverRunnable(threadHealthReporter, metricsObserverRunner);
    ScheduledFuture<?> metricObserverFuture = executor.scheduleWithFixedDelayReturnFuture(
      metricObserverRunnable, 1, 2, TimeUnit.SECONDS);

    observerRunnable = new DataObserverRunnable(threadHealthReporter, this.getMetrics(), productionObserveRequests,
      alertManager, configuration);
    Future<?> observerFuture = executor.submitReturnFuture(observerRunnable);

    pipelineRunnable = new ProductionPipelineRunnable(threadHealthReporter, this, prodPipeline, name, rev,
      ImmutableList.of(configLoaderFuture, observerFuture, metricObserverFuture));

    executor.submit(pipelineRunnable);

    LOG.debug("Started pipeline {} {}", name, rev);
  }

  private void handleStopRequest(boolean nodeProcessShutdown) {
    LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
    if(pipelineRunnable != null) {
      pipelineRunnable.stop(nodeProcessShutdown);
      pipelineRunnable = null;
    }
    threadHealthReporter.destroy();
    threadHealthReporter = null;
    LOG.debug("Stopped pipeline");
  }

  @VisibleForTesting
  ProductionPipeline createProductionPipeline(String name, String rev,
                                              com.streamsets.pipeline.util.Configuration configuration,
                                              StageLibraryTask stageLibrary, ProductionObserver observer,
                                              PipelineConfiguration pipelineConfiguration,
                                              BlockingQueue<Object> observeRequests)
    throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {

    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    for(ConfigConfiguration config : pipelineConfiguration.getConfiguration()) {
      if(Configuration.DELIVERY_GUARANTEE.equals(config.getName())) {
        deliveryGuarantee = DeliveryGuarantee.valueOf((String)config.getValue());
      }
    }
    //create the pipeline directory eagerly.
    //This helps avoid race conditions when different stores attempt to create directories
    //Creating directory eagerly also avoids the need of synchronization
    createPipelineDirIfNotExist(name);
    stateTracker.register(name, rev);

    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, snapshotStore,
      deliveryGuarantee, name, rev, observeRequests, configuration, getMemoryLimitConfiguration(pipelineConfiguration));

    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, rev, runtimeInfo,
      pipelineConfiguration);
    return builder.build(runner, offsetTracker, observer);
  }

  private MemoryLimitConfiguration getMemoryLimitConfiguration(PipelineConfiguration pipelineConfiguration)
    throws PipelineRuntimeException {
    //Default memory limit configuration
    MemoryLimitConfiguration memoryLimitConfiguration = new MemoryLimitConfiguration();

    List<ConfigConfiguration> configuration = pipelineConfiguration.getConfiguration();
    MemoryLimitExceeded memoryLimitExceeded = null;
    long memoryLimit = 0;

    if (configuration != null) {
      for (ConfigConfiguration config : configuration) {
        if (PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG.equals(config.getName())) {
          try {
            memoryLimitExceeded = MemoryLimitExceeded.valueOf(String.valueOf(config.getValue()).
              toUpperCase(Locale.ENGLISH));
          } catch (IllegalArgumentException e) {
            //This should never happen.
            String msg = "Invalid pipeline configuration: " + PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG +
              " value: '" + config.getValue() + "'. Should never happen, please report. : " + e;
            throw new IllegalStateException(msg, e);
          }
        } else if (PipelineDefConfigs.MEMORY_LIMIT_CONFIG.equals(config.getName())) {
          String memoryLimitString = String.valueOf(config.getValue());

          if(ElUtil.isElString(memoryLimitString)) {
            //Memory limit is an EL expression. Evaluate to get the value
            try {
              memoryLimit = ValidationUtil.evaluateMemoryLimit(memoryLimitString, ElUtil.getConstants(pipelineConfiguration));
            } catch (ELEvalException e) {
              throw new PipelineRuntimeException(ValidationError.VALIDATION_0064, e.getMessage(), e);
            }
          } else {
            //Memory limit is not an EL expression. Parse it as long.
            try {
              memoryLimit = Long.parseLong(memoryLimitString);
            } catch (NumberFormatException e) {
              throw new PipelineRuntimeException(ValidationError.VALIDATION_0062, memoryLimitString);
            }
          }

          if (memoryLimit > PipelineDefConfigs.MEMORY_LIMIT_MAX) {
            throw new PipelineRuntimeException(ValidationError.VALIDATION_0063, memoryLimit,
              "above the maximum", PipelineDefConfigs.MEMORY_LIMIT_MAX);
          }
        }
      }
    }
    if (memoryLimitExceeded != null && memoryLimit > 0) {
      memoryLimitConfiguration = new MemoryLimitConfiguration(memoryLimitExceeded,
        memoryLimit * 1000 * 1000 /*convert MB to bytes*/);
    }
    return memoryLimitConfiguration;
  }

  private void createPipeline(String name, String rev, ProductionObserver observer, BlockingQueue<Object> observeRequests)
    throws PipelineStoreException, PipelineManagerException, StageException, PipelineRuntimeException {
    PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);
    prodPipeline = createProductionPipeline(name, rev, configuration, stageLibrary,
      observer, pipelineConfiguration, observeRequests);
  }

  @VisibleForTesting
  public StateTracker getStateTracker() {
    return stateTracker;
  }

  public void validateStateTransition(String pipelineName, String rev, State toState) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    PipelineState ps = getPipelineState();
    if(ps != null) {
      checkState(VALID_TRANSITIONS.get(ps.getState()).contains(toState), ContainerError.CONTAINER_0102, ps.getState()
        , toState);
    }
  }

  private void checkState(boolean expr, ContainerError error, Object... args)
      throws PipelineManagerException {
    if(!expr) {
      throw new PipelineManagerException(error, args);
    }
  }

  private void validatePipelineExistence(String pipelineName) throws PipelineManagerException {
    if(!pipelineStore.hasPipeline(pipelineName)) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0109, pipelineName);
    }
  }

  private void createPipelineDirIfNotExist(String name) throws PipelineManagerException {
    File pipelineDir = new File(new File(runtimeInfo.getDataDir(), RUN_INFO_DIR),
      PipelineDirectoryUtil.getEscapedPipelineName(name));
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new PipelineManagerException(ContainerError.CONTAINER_0110, name,
          Utils.format("'{}' mkdir failed", pipelineDir));
      }
    }
  }

  public void deleteHistory(String pipelineName, String rev) throws PipelineManagerException {
    LOG.debug("Deleting history for pipeline {}", pipelineName);
    PipelineState pState = getPipelineState();
    if(pState == null) {
      return;
    }
    if(pState.getName().equals(pipelineName) && pState.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0111, pipelineName);
    }
    stateTracker.deleteHistory(pipelineName, rev);
  }

  public boolean deleteAlert(String alertId) throws PipelineManagerException {
    checkState(getPipelineState().getState().equals(State.RUNNING), ContainerError.CONTAINER_0402);
    return MetricsConfigurator.removeGauge(getMetrics(), AlertsUtil.getAlertGaugeName(alertId));
  }

  @VisibleForTesting
  String getOffset() {
    return prodPipeline.getCommittedOffset();
  }
}
