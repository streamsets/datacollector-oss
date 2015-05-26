/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.validation.constraints.NotNull;

import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.updatechecker.UpdateChecker;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.cluster.ApplicationState;
import com.streamsets.pipeline.cluster.ClusterModeConstants;
import com.streamsets.pipeline.cluster.SparkManager;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.metrics.MetricsEventRunnable;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.runner.production.ProductionPipelineBuilder;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunner;
import com.streamsets.pipeline.snapshotstore.SnapshotInfo;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineConfigurationUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.validation.Issues;
import com.streamsets.pipeline.validation.StageIssue;
import com.streamsets.pipeline.validation.ValidationError;

public class ClusterPipelineManager extends AbstractTask implements PipelineManager {
  private static final String CLUSTER_PIPELINE_MANAGER = "ClusterPipelineManager";
  private static final Logger LOG = LoggerFactory.getLogger(ClusterPipelineManager.class);
  static final String APPLICATION_STATE = "cluster.application.state";
  private static final long SUBMIT_TIMEOUT_SECS = 60;

  static final Map<State, Set<State>> VALID_TRANSITIONS = new ImmutableMap.Builder<State, Set<State>>()
    .put(State.STOPPED, ImmutableSet.of(State.RUNNING))
    .put(State.RUNNING, ImmutableSet.of(State.STOPPING, State.ERROR))
    .put(State.STOPPING, ImmutableSet.of(State.STOPPING /*Try stopping many times, this should be no-op*/
      , State.STOPPED, State.ERROR))
    .put(State.ERROR, ImmutableSet.of(State.RUNNING, State.STOPPED))
    .build();

  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final StageLibraryTask stageLibrary;
  private final StateTracker stateTracker;
  private final ManagerRunnable managerRunnable;
  private final SafeScheduledExecutorService scheduledExecutor;
  private final ExecutorService executor;
  private final File tempDir;
  private SparkManager sparkManager;
  private Cache<String, CallbackInfo> slaveCallbackList;
  private MetricsEventRunnable metricsEventRunnable;
  private final ReentrantLock callbackCacheLock;

  private UpdateChecker updateChecker;

  public ClusterPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration, PipelineStoreTask pipelineStore,
                                StageLibraryTask stageLibrary) {
    this(runtimeInfo, configuration, pipelineStore, stageLibrary, null,
      new StateTracker(runtimeInfo, configuration));
  }

  @VisibleForTesting
  ClusterPipelineManager(RuntimeInfo runtimeInfo, Configuration configuration, PipelineStoreTask pipelineStore,
      StageLibraryTask stageLibrary, SparkManager sparkManager, StateTracker stateTracker) {
    super(ClusterPipelineManager.class.getSimpleName());
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    this.stateTracker = stateTracker;
    this.tempDir = Files.createTempDir();
    this.sparkManager = sparkManager;
    this.scheduledExecutor = new SafeScheduledExecutorService(4, CLUSTER_PIPELINE_MANAGER);
    this.executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "ClusterPipelineManager-ManagerRunnable");
        thread.setDaemon(true);
        return thread;
      }
    });
    if (this.sparkManager == null) {
      this.sparkManager = new SparkManager(runtimeInfo, tempDir);
    }
    this.managerRunnable = new ManagerRunnable(this, stateTracker, this.sparkManager);
    this.executor.submit(managerRunnable);
    slaveCallbackList = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
    callbackCacheLock = new ReentrantLock();
  }

  @Override
  protected void initTask() {
    stateTracker.init();
    final PipelineState ps = getPipelineState();
    LOG.info("State on initTask: " + ps);
    if (ps != null && ps.getState() == State.RUNNING) {
      final Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(ps.getAttributes());
      ApplicationState appState = new ApplicationState((Map) attributes.get(APPLICATION_STATE));
      if (appState.getId() == null) {
        String msg = "Pipeline is supposed to be running but not application id can be found";
        transitionToError(ps, msg);
      } else {
        runtimeInfo.setSDCToken(appState.getSdcToken());
        try {
          startPipeline(ps.getName(), ps.getRev());
        } catch (Exception ex) {
          String msg = "Error starting pipeline: " + ex;
          LOG.error(msg, ex);
          transitionToError(ps, msg);
        }

      }
    }
    int refreshInterval = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);
    if (refreshInterval > 0) {
      metricsEventRunnable = new MetricsEventRunnable(this, runtimeInfo, refreshInterval);
      scheduledExecutor.scheduleAtFixedRate(metricsEventRunnable, 0, refreshInterval, TimeUnit.MILLISECONDS);
    }

    // update checker
    updateChecker = new UpdateChecker(runtimeInfo, configuration, this);
    scheduledExecutor
        .scheduleAtFixedRate(new UpdateChecker(runtimeInfo, configuration, this), 1, 24 * 60, TimeUnit.MINUTES);
  }

  public Map getUpdateInfo() {
    return updateChecker.getUpdateInfo();
  }

  private void transitionToError(PipelineState ps, String msg) {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.putAll(ps.getAttributes());
    try {
      validateStateTransition(ps.getName(), ps.getRev(), State.ERROR);
      attributes.remove(APPLICATION_STATE);
      stateTracker.setState(ps.getName(), ps.getRev(), State.ERROR, msg, null, attributes);
    } catch(Exception ex) {
      LOG.error("Error transitioning to {}: {}", State.ERROR, ex, ex);
    }
  }

  @Override
  protected void stopTask() {
    executor.shutdownNow();
    scheduledExecutor.shutdownNow();
    // DO nothing, SDC goes down but pipeline should continue running
    FileUtils.deleteQuietly(tempDir);
   }

  @Override
  public PipelineState getPipelineState() {
    return stateTracker.getState();
  }

  @Override
  public void addStateEventListener(StateEventListener stateListener) {
    stateTracker.addStateEventListener(stateListener);
  }

  @Override
  public void removeStateEventListener(StateEventListener stateListener) {
    stateTracker.removeStateEventListener(stateListener);
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventRunnable.addMetricsEventListener(metricsEventListener);
  }

  @Override
  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventRunnable.removeMetricsEventListener(metricsEventListener);
  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetOffset(String pipelineName, String rev) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void captureSnapshot(String snapshotName, int batchSize) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SnapshotStatus getSnapshotStatus(String snapshotName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getSnapshot(String pipelineName, String rev, String snapshotName) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Record> getErrorRecords(String instanceName, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Record> getSampledRecords(String sampleDefinitionId, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String instanceName, int size) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) throws
      PipelineManagerException {
    if(!pipelineStore.hasPipeline(pipelineName)) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0109, pipelineName);
    }
    return stateTracker.getHistory(pipelineName, rev, fromBeginning);
  }

  @Override
  public void deleteSnapshot(String pipelineName, String rev, String snapshotName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineState startPipeline(final String name, final String rev) throws PipelineStoreException,
    PipelineManagerException, PipelineRuntimeException, StageException {
    //create the pipeline directory eagerly.
    //This helps avoid race conditions when different stores attempt to create directories
    //Creating directory eagerly also avoids the need of synchronization
    createPipelineDirIfNotExist(name);
    PipelineState ps = stateTracker.getState();
    LOG.debug("State of pipeline is " + ps);
    PipelineConfiguration pipelineConf = pipelineStore.load(name, rev);
    ExecutionMode executionMode = ExecutionMode.valueOf((String) pipelineConf.getConfiguration(
      PipelineDefConfigs.EXECUTION_MODE_CONFIG).getValue());
    if (executionMode != ExecutionMode.CLUSTER) {
      throw new PipelineManagerException(ValidationError.VALIDATION_0073);
    }
    if (ps == null) {
      validatePipelineExistence(name);
    } else if (ps.getState() != State.RUNNING) {
      validateStateTransition(name, rev, State.RUNNING);
    }
    ApplicationState appState;
    if (ps == null) {
      stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, null);
      appState = new ApplicationState();
    } else {
      appState = new ApplicationState((Map) ps.getAttributes().get(APPLICATION_STATE));
      if (metricsEventRunnable != null) {
        metricsEventRunnable.clearSlaveMetrics();
      }
      stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, ps.getAttributes());
    }
    managerRunnable.requestTransition(State.RUNNING, appState, pipelineConf);
    return stateTracker.getState();
  }

  @Override
  public PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException {
    final PipelineState ps = stateTracker.getState();
    if (!nodeProcessShutdown) {
      if (ps == null) {
        throw new PipelineManagerException(ContainerError.CONTAINER_0101, "for pipeline");
      } else {
        switch (ps.getState()) {
          case STOPPED:
          case STOPPING:
            // do nothing
            break;
          case RUNNING:
            tryStopPipeline();
            break;
          default:
            throw new PipelineManagerException(ContainerError.CONTAINER_0102, ps.getState(), State.STOPPED);

        }
      }
    }
    return stateTracker.getState();
  }

  /**
   * Block for up to 10 seconds while attempting to get an application id.
   * Provides a better behavior when trying to stop a pipeline which just
   * started without introducing a new state.
   */
  private void tryStopPipeline() throws PipelineManagerException {
    long start = System.currentTimeMillis();
    String appId;
    PipelineState ps;
    do {
      ps = Utils.checkNotNull(stateTracker.getState(), "Pipeline state cannot be null");
      ApplicationState appState = new ApplicationState((Map)ps.getAttributes().get(APPLICATION_STATE));
      appId = appState.getId();
      if (appId == null) {
        Utils.checkState(ThreadUtil.sleep(500), "Interrupted while sleeping");
      } else {
        break;
      }
    } while (TimeUnit.MILLISECONDS.toSeconds(Math.max(0, System.currentTimeMillis() - start)) < 30);
    if (appId == null) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0101, "for cluster application");
    } else {
      ApplicationState appState = new ApplicationState((Map)ps.getAttributes().get(APPLICATION_STATE));
      validateStateTransition(ps.getName(), ps.getRev(), State.STOPPING);
      stateTracker.setState(ps.getName(), ps.getRev(), State.STOPPING, "Stopping cluster pipeline", null,
        ps.getAttributes());
      PipelineConfiguration pipelineConf = null;
      try {
        pipelineConf = pipelineStore.load(ps.getName(), ps.getRev());
      } catch (PipelineStoreException e) {
        String msg = Utils.format(ContainerError.CONTAINER_0150.getMessage(), e);
        transitionToError(ps, msg);
        throw new PipelineManagerException(ContainerError.CONTAINER_0150, ps.getState(), e.toString(), e);
      }
      managerRunnable.requestTransition(State.STOPPED, appState, pipelineConf);
    }
  }
  @Override
  public Object getMetrics() {
    return metricsEventRunnable.getAggregatedMetrics();
  }

  @Override
  public void deleteHistory(String pipelineName, String rev) throws PipelineManagerException {
    LOG.debug("Deleting history for pipeline {}", pipelineName);
    PipelineState state = getPipelineState();
    if(state != null && state.getName().equals(pipelineName) && state.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0111, pipelineName);
    }
    stateTracker.deleteHistory(pipelineName, rev);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineManagerException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProductionPipeline getProductionPipeline() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    String sdcToken = Strings.nullToEmpty(runtimeInfo.getSDCToken());
    if (sdcToken.equals(callbackInfo.getSdcClusterToken()) &&
      !RuntimeInfo.UNDEF.equals(callbackInfo.getSdcURL())) {
      callbackCacheLock.lock();
      try {
        slaveCallbackList.put(callbackInfo.getSdcURL(), callbackInfo);
      } finally {
         callbackCacheLock.unlock();
      }
    } else {
      LOG.warn("SDC Cluster token not matched");
    }
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    List<CallbackInfo> callbackInfoSet;
    callbackCacheLock.lock();
    try {
      callbackInfoSet = new ArrayList<>(slaveCallbackList.asMap().values());
    } finally {
      callbackCacheLock.unlock();
    }
    return callbackInfoSet;
  }

  void clearSlaveList() {
    callbackCacheLock.lock();
    try {
      slaveCallbackList.invalidateAll();
    } finally {
      callbackCacheLock.unlock();
    }
  }

  private void createPipelineDirIfNotExist(String name) throws PipelineManagerException {
    File pipelineDir = new File(new File(runtimeInfo.getDataDir(), StandalonePipelineManagerTask.RUN_INFO_DIR),
      PipelineDirectoryUtil.getEscapedPipelineName(name));
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new PipelineManagerException(ContainerError.CONTAINER_0110, name,
          Utils.format("'{}' mkdir failed", pipelineDir));
      }
    }
  }

  @VisibleForTesting
  ManagerRunnable getManagerRunnable() {
    return managerRunnable;
  }

  @VisibleForTesting
  int getOriginParallelism(String name, String rev, PipelineConfiguration pipelineConf)
    throws PipelineRuntimeException, StageException, PipelineStoreException, PipelineManagerException {

    ProductionPipeline p = createProductionPipeline(name, rev, pipelineConf);
    p.getPipeline().init();

    int parallelism = p.getPipeline().getSource().getParallelism();
    if(parallelism < 1) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0112);
    }
    return parallelism;
  }

  private ProductionPipeline createProductionPipeline(String name, String rev,
                                                      PipelineConfiguration pipelineConfiguration)
    throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {
    ProductionPipelineRunner runner = new ProductionPipelineRunner(runtimeInfo, null,
      DeliveryGuarantee.AT_LEAST_ONCE, name, rev, null, null, new MemoryLimitConfiguration());
    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, rev, runtimeInfo,
      pipelineConfiguration);
    return builder.build(runner, null, null);
  }


  private void validateStateTransition(String pipelineName, String rev, State toState)
    throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    PipelineState ps = stateTracker.getState();
    if (ps != null) {
      checkState(VALID_TRANSITIONS.get(ps.getState()).contains(toState),
        ContainerError.CONTAINER_0102, ps.getState(), toState);
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

  @VisibleForTesting
  ExecutorService getManagerExecutor() {
    return executor;
  }

  @VisibleForTesting
  public static class StateTransitionRequest {
    final State canidateState;
    final ApplicationState applicationState;
    final PipelineConfiguration pipelineConf;

    public StateTransitionRequest(@NotNull State canidateState, @NotNull ApplicationState applicationState,
                                  PipelineConfiguration pipelineConf) {
      this.canidateState = canidateState;
      this.applicationState = applicationState;
      this.pipelineConf = pipelineConf;
    }

    @Override
    public String toString() {
      return "StateTransitionRequest{" +
        "canidateState=" + canidateState +
        ", applicationState=" + applicationState +
        '}';
    }
  }
  public static class ManagerRunnable implements Runnable {
    final BlockingQueue<Optional<StateTransitionRequest>> queue = new ArrayBlockingQueue<>(10);
    final ClusterPipelineManager clusterPipelineManager;
    final StateTracker stateTracker;
    final SparkManager sparkManager;
    final PipelineStoreTask pipelineStore;

    public ManagerRunnable(ClusterPipelineManager clusterPipelineManager, StateTracker stateTracker,
                           SparkManager sparkManager) {
      this.clusterPipelineManager = clusterPipelineManager;
      this.stateTracker = stateTracker;
      this.sparkManager = sparkManager;
      this.pipelineStore = clusterPipelineManager.pipelineStore;
    }

    @Override
    public void run() {
      while (true) {
        try {
          Optional<StateTransitionRequest> optionalRequest = queue.poll(30, TimeUnit.SECONDS);
          if (optionalRequest == null || !optionalRequest.isPresent()) {
            checkStatus();
          } else {
            handleStateTransitionRequest(optionalRequest.get());
          }
        } catch (InterruptedException ex) {
          break;
        } catch (Throwable throwable) {
          String msg = "Unexpected error: " + throwable;
          LOG.error(msg, throwable);
        }
      }
    }

    @VisibleForTesting
    void forceCheckPipelineState() {
      Utils.checkState(queue.add(Optional.<StateTransitionRequest>absent()), "Could not add to queue");
    }

    private void requestTransition(State canidateState, ApplicationState applicationState,
                                  PipelineConfiguration pipelineConfiguration) {
      StateTransitionRequest request = new StateTransitionRequest(canidateState, applicationState, pipelineConfiguration);
      if(!queue.add(Optional.of(request))) {
        LOG.error(Utils.format("Could not add state transition request to queue: {}", request));
      }
    }

    @VisibleForTesting
    void checkStatus() throws PipelineManagerException {
      Boolean running = null;
      PipelineState ps = stateTracker.getState();
      if (ps != null && ps.getState() == State.RUNNING) {
        ApplicationState appState = new ApplicationState((Map)ps.getAttributes().get(APPLICATION_STATE));
        try {
          PipelineConfiguration pipelineConf = pipelineStore.load(ps.getName(), ps.getRev());
          running = sparkManager.isRunning(appState, pipelineConf).get(60, TimeUnit.SECONDS);
        } catch (Exception ex) {
          String msg = "Error getting application status: " + ex;
          LOG.warn(msg, ex);
        }
        if (running == null) {
          // error occurred, do nothing
        } else if (!running) {
          String msg = "Pipeline unexpectedly stopped";
          clusterPipelineManager.transitionToError(ps, msg);
        }
      }
    }


    private void handleStateTransitionRequest(StateTransitionRequest request) throws PipelineManagerException {
      LOG.info("Attempting to transition to: " + request);
      switch (request.canidateState) {
        case STOPPED:
          stop(request);
          break;
        case RUNNING:
          start(request);
          break;
        default:
          throw new IllegalStateException(Utils.format("Canidate state {} is not support", request.canidateState));
      }
    }

    @VisibleForTesting
    void start(StateTransitionRequest request) throws PipelineManagerException {
      ApplicationState appState = request.applicationState;
      PipelineState ps = stateTracker.getState();
      Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(ps.getAttributes());
      PipelineConfiguration pipelineConf = Utils.checkNotNull(request.pipelineConf, "PipelineConfiguration cannot be null");
      if (appState.getId() == null) {
        doStart(ps.getName(), ps.getRev(), pipelineConf);
      } else {
        Boolean running = null;
        try {
          running = sparkManager.isRunning(appState, pipelineConf).get(60, TimeUnit.SECONDS);
        } catch (Exception ex) {
          String msg = "Error getting application status: " + ex;
          clusterPipelineManager.transitionToError(ps, msg);
          LOG.error(msg, ex);
        }

        if (running == null) {
          // error occurred, do nothing
        } else if (running) {
          if (ps.getState() != State.RUNNING) {
            clusterPipelineManager.validateStateTransition(ps.getName(), ps.getRev(), State.RUNNING);
            stateTracker.setState(ps.getName(), ps.getRev(), State.RUNNING, null, null, attributes);
          }
        } else if (ps.getState() == State.RUNNING) {
          // not running but state is running, this is an error
          String msg = "Pipeline is supposed to be running but has died";
          clusterPipelineManager.transitionToError(ps, msg);
        } else {
          doStart(ps.getName(), ps.getRev(), pipelineConf);
        }
      }
    }

    private void doStart(String name, String rev, PipelineConfiguration pipelineConf) throws PipelineManagerException {
      stateTracker.register(name, rev);
      Map<String, String> environment = new HashMap<>();
      Map<String, String> envConfigMap = PipelineConfigurationUtil.getFlattenedStringMap(PipelineDefConfigs.
        CLUSTER_LAUNCHER_ENV_CONFIG, pipelineConf);
      environment.putAll(envConfigMap);
      Map<String, String> sourceInfo = new HashMap<>();
      File bootstrapDir = new File(this.clusterPipelineManager.runtimeInfo.getLibexecDir(),
        "bootstrap-libs");
      PipelineState ps = stateTracker.getState();
      try {
        //create pipeline and get the parallelism info from the source
        String parallelism = String.valueOf(clusterPipelineManager.getOriginParallelism(name, rev, pipelineConf));
        sourceInfo.put(ClusterModeConstants.NUM_EXECUTORS_KEY, parallelism);
        //This is needed for UI
        RuntimeInfo runtimeInfo = clusterPipelineManager.runtimeInfo;
        runtimeInfo.setAttribute(ClusterModeConstants.NUM_EXECUTORS_KEY, parallelism);
        clusterPipelineManager.clearSlaveList();
        ListenableFuture<ApplicationState> submitFuture = sparkManager.submit(pipelineConf, clusterPipelineManager.stageLibrary,
          new File(runtimeInfo.getConfigDir()), new File(runtimeInfo.getStaticWebDir()), bootstrapDir, environment,
          sourceInfo, SUBMIT_TIMEOUT_SECS);
        // set state of running before adding callback which modified attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.putAll(ps.getAttributes());
        // add an extra 10 sec wait to the actual timeout value passed to SparkManager#submit
        ApplicationState applicationState = submitFuture.get(SUBMIT_TIMEOUT_SECS + 10, TimeUnit.SECONDS);
        attributes.put(APPLICATION_STATE, applicationState.getMap());
        stateTracker.setState(name, rev, State.RUNNING, "Starting cluster pipeline", null, attributes);
      } catch (Exception ex) {
        // TODO if PipelineRuntimeException fails we should send validation issues back to client
        String msg = "Error starting application: " + ex;
        clusterPipelineManager.transitionToError(ps, msg);
        LOG.error(msg, ex);
      }
    }

    @VisibleForTesting
    void stop(StateTransitionRequest request) throws PipelineManagerException {
      Utils.checkState(request.applicationState != null, "Application state cannot be null");
      final PipelineState ps = stateTracker.getState();
      boolean stopped = false;
      Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(ps.getAttributes());
      attributes.remove(APPLICATION_STATE);
      try {
        sparkManager.kill(request.applicationState, request.pipelineConf).get(60, TimeUnit.SECONDS);
        stopped = true;
      } catch (Exception ex) {
        String msg = "Error stopping cluster: " + ex;
        LOG.error(msg, ex);
        clusterPipelineManager.validateStateTransition(ps.getName(), ps.getRev(), State.ERROR);
        stateTracker.setState(ps.getName(), ps.getRev(), State.ERROR, msg, null, attributes);
      }
      if (stopped) {
        clusterPipelineManager.validateStateTransition(ps.getName(), ps.getRev(), State.STOPPED);
        stateTracker.setState(ps.getName(), ps.getRev(), State.STOPPED, "Stopped cluster pipeline", null,
          attributes);
      }
    }
  }
}
