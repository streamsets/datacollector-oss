/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.errorrecordstore.ErrorRecordStore;
import com.streamsets.pipeline.errorrecordstore.impl.FileErrorRecordStore;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.runner.production.ProductionPipelineBuilder;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunnable;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunner;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProductionPipelineManagerTask extends AbstractTask {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineManagerTask.class);
  private static final String PRODUCTION_PIPELINE_MANAGER = "productionPipelineManager";
  private static final String PRODUCTION_PIPELINE_RUNNER = "ProductionPipelineRunner";
  private static final String RUN_INFO_DIR = "runInfo";

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
  private final ErrorRecordStore errorRecordStore;

  /*References the thread that is executing the pipeline currently */
  private ProductionPipelineRunnable pipelineRunnable;
  /*The executor service that is currently executing the ProdPipelineRunnerThread*/
  private ExecutorService executor;
  /*The pipeline being executed or the pipeline in the context*/
  private ProductionPipeline prodPipeline;

  /*Mutex objects to synchronize start and stop pipeline methods*/
  private final Object pipelineMutex = new Object();

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
    errorRecordStore = new FileErrorRecordStore(runtimeInfo, configuration);
  }


  public PipelineState getPipelineState() {
    return stateTracker.getState();
  }

  public void setState(String name, String rev, State state, String message) throws PipelineManagerException {
    stateTracker.setState(name, rev, state, message);
  }

  @Override
  public void initTask() {
    LOG.debug("Initializing Production Pipeline Manager");
    stateTracker.init();
    executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(PRODUCTION_PIPELINE_RUNNER)
        .setDaemon(true).build());
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
            prodPipeline = createProductionPipeline(ps.getName(), ps.getRev(), configuration, pipelineStore,
              stageLibrary);
          } catch (Exception e) {
            //log error and shutdown again
            LOG.error(ContainerError.CONTAINER_0108.getMessage(), e.getMessage());
          }
      }
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

  public void captureSnapshot(int batchSize) throws PipelineManagerException {
    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    checkState(getPipelineState() != null && getPipelineState().getState().equals(State.RUNNING),
      ContainerError.CONTAINER_0105);
    if(batchSize <= 0) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0107, batchSize);
    }
    prodPipeline.captureSnapshot(batchSize);
    LOG.debug("Captured snapshot with batch size {}", batchSize);
  }

  public SnapshotStatus getSnapshotStatus() {
    return snapshotStore.getSnapshotStatus(stateTracker.getState().getName(), stateTracker.getState().getRev());
  }

  public InputStream getSnapshot(String pipelineName, String rev) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return snapshotStore.getSnapshot(pipelineName, rev);
  }

  public InputStream getErrors(String pipelineName, String rev) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return errorRecordStore.getErrors(pipelineName, rev);
  }

  public List<Record> getErrorRecords(String instanceName) throws PipelineManagerException {
    checkState(getPipelineState().getState().equals(State.RUNNING), ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorRecords(instanceName);
  }

  public List<ErrorMessage> getErrorMessages(String instanceName) throws PipelineManagerException {
    checkState(getPipelineState() != null && getPipelineState().getState().equals(State.RUNNING),
      ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorMessages(instanceName);
  }

  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return stateTracker.getHistory(pipelineName, rev, fromBeginning);
  }

  public void deleteSnapshot(String pipelineName, String rev) {
    LOG.debug("Deleting snapshot");
    snapshotStore.deleteSnapshot(pipelineName, rev);
    LOG.debug("Deleted snapshot");
  }


  public PipelineState startPipeline(String name, String rev) throws PipelineStoreException
      , PipelineManagerException, PipelineRuntimeException, StageException {
    synchronized (pipelineMutex) {
      LOG.info("Starting pipeline {} {}", name, rev);
      validateStateTransition(name, rev, State.RUNNING);
      handleStartRequest(name, rev);
      setState(name, rev, State.RUNNING, null);
      return getPipelineState();
    }
  }

  public PipelineState stopPipeline(boolean nodeProcessShutdown) throws PipelineManagerException {
    synchronized (pipelineMutex) {
      validateStateTransition(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING);
      setState(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING,
        Configuration.STOP_PIPELINE_MESSAGE);
      PipelineState pipelineState = getPipelineState();
      handleStopRequest(nodeProcessShutdown);
      return pipelineState;
    }
  }

  public MetricRegistry getMetrics() {
    return prodPipeline.getPipeline().getRunner().getMetrics();
  }

  private void handleStartRequest(String name, String rev) throws PipelineManagerException, StageException
      , PipelineRuntimeException, PipelineStoreException {
    prodPipeline = createProductionPipeline(name, rev, configuration, pipelineStore, stageLibrary);
    pipelineRunnable = new ProductionPipelineRunnable(this, prodPipeline, name, rev);
    executor.submit(pipelineRunnable);
    LOG.debug("Started pipeline {} {}", name, rev);
  }

  private void handleStopRequest(boolean nodeProcessShutdown) {
    LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
    if(pipelineRunnable != null) {
      pipelineRunnable.stop(nodeProcessShutdown);
      pipelineRunnable = null;
    }
    LOG.debug("Stopped pipeline");
  }

  @VisibleForTesting
  ProductionPipeline createProductionPipeline(String name, String rev
      , com.streamsets.pipeline.util.Configuration configuration, PipelineStoreTask pipelineStore
      , StageLibraryTask stageLibrary) throws PipelineStoreException, PipelineRuntimeException, StageException
      , PipelineManagerException {

    //retrieve pipeline properties from the pipeline configuration
    int maxBatchSize = configuration.get(Configuration.MAX_BATCH_SIZE_KEY, Configuration.MAX_BATCH_SIZE_DEFAULT);
    int maxErrorRecordsPerStage = configuration.get(Configuration.MAX_ERROR_RECORDS_PER_STAGE_KEY,
      Configuration.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT);
    int maxPipelineErrors = configuration.get(Configuration.MAX_PIPELINE_ERRORS_KEY,
      Configuration.MAX_PIPELINE_ERRORS_DEFAULT);

    //load pipeline configuration from store
    PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);
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
    //register the pipeline with the error record store
    errorRecordStore.register(name, rev);
    stateTracker.register(name, rev);

    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, rev, runtimeInfo);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore, errorRecordStore, offsetTracker,
        maxBatchSize, maxErrorRecordsPerStage, maxPipelineErrors, deliveryGuarantee, name, rev);
    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, pipelineConfiguration);

    return builder.build(runner);
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

  public void deleteErrors(String pipelineName, String rev) throws PipelineManagerException {
    LOG.debug("Deleting errors for pipeline {}", pipelineName);
    PipelineState pState = getPipelineState();
    if(pState == null) {
      return;
    }
    if(pState.getName().equals(pipelineName) && pState.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0111, pipelineName);
    }
    errorRecordStore.deleteErrors(pipelineName, rev);
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

  @VisibleForTesting
  String getOffset() {
    return prodPipeline.getCommittedOffset();
  }
}
