/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.container.Utils;
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
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;
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

  private static final Map<State, Set<State>> VALID_TRANSITIONS = ImmutableMap.of(
      State.STOPPED, (Set<State>) ImmutableSet.of(State.RUNNING),
      State.FINISHED, (Set<State>) ImmutableSet.of(State.RUNNING),
      State.RUNNING, (Set<State>) ImmutableSet.of(State.STOPPING, State.FINISHED),
      State.STOPPING, (Set<State>) ImmutableSet.of(State.STOPPING /*Try stopping many times, this should be no-op*/
          , State.STOPPED),
      State.ERROR, (Set<State>) ImmutableSet.of(State.RUNNING, State.STOPPED));

  private final RuntimeInfo runtimeInfo;
  private final StateTracker stateTracker;
  private final Configuration configuration;
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
  public ProductionPipelineManagerTask(RuntimeInfo runtimeInfo, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
    super(PRODUCTION_PIPELINE_MANAGER);
    this.runtimeInfo = runtimeInfo;
    stateTracker = new StateTracker(runtimeInfo);
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    snapshotStore = new FileSnapshotStore(runtimeInfo);
    errorRecordStore = new FileErrorRecordStore(runtimeInfo);
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
    if(State.RUNNING.equals(ps.getState())) {
      try {
        LOG.debug("Starting pipeline {} {}", ps.getName(), ps.getRev());
        handleStartRequest(ps.getName(), ps.getRev());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      //create the pipeline instance. This was the pipeline in the context before the manager shutdown previously
      try {
        prodPipeline = createProductionPipeline(ps.getName(), ps.getRev(), configuration, pipelineStore, stageLibrary);
      } catch (Exception e) {
        //log error and shutdown again
        LOG.error(ContainerError.CONTAINER_0108.getMessage(), e.getMessage());
      }
    }
    LOG.debug("Initialized Production Pipeline Manager");
  }

  @Override
  public void stopTask() {
    LOG.debug("Stopping Production Pipeline Manager");
    PipelineState ps = getPipelineState();
    if(State.RUNNING.equals(ps.getState())) {
      LOG.debug("Stopping pipeline {} {}", ps.getName(), ps.getRev());
      try {
        stopPipeline();
      } catch (PipelineManagerException e) {
        throw new RuntimeException(e);
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

  public String setOffset(String offset) throws PipelineManagerException {
    LOG.debug("Setting offset {}", offset);
    checkState(!getPipelineState().getState().equals(State.RUNNING),
          ContainerError.CONTAINER_0103);

    prodPipeline.setOffset(offset);
    return prodPipeline.getCommittedOffset();
  }

  public void resetOffset(String pipelineName) throws PipelineManagerException {
    LOG.debug("Resetting offset for pipeline {}", pipelineName);
    PipelineState pState = getPipelineState();
    if(pState.getName().equals(pipelineName) && pState.getState() == State.RUNNING) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0104,
          pipelineName);
    }
    createPipelineDirIfNotExist(pipelineName);
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(pipelineName, runtimeInfo);
    offsetTracker.resetOffset(pipelineName);
  }

  public void captureSnapshot(int batchSize) throws PipelineManagerException {
    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    checkState(getPipelineState().getState().equals(State.RUNNING),
        ContainerError.CONTAINER_0105);
    if(batchSize <= 0) {
      throw new PipelineManagerException(ContainerError.CONTAINER_0107, batchSize);
    }
    prodPipeline.captureSnapshot(batchSize);
    LOG.debug("Captured snapshot with batch size {}", batchSize);
  }

  public SnapshotStatus getSnapshotStatus() {
    return snapshotStore.getSnapshotStatus(stateTracker.getState().getName());
  }

  public InputStream getSnapshot(String pipelineName) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return snapshotStore.getSnapshot(pipelineName);
  }

  public InputStream getErrorRecords(String pipelineName, String rev, String stageInstanceName) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return errorRecordStore.getErrorRecords(pipelineName, rev, stageInstanceName);
  }

  public List<Record> getErrorRecords(String instanceName) throws PipelineManagerException {
    checkState(getPipelineState().getState().equals(State.RUNNING),
        ContainerError.CONTAINER_0106);
    return prodPipeline.getErrorRecords(instanceName);
  }

  public List<PipelineState> getHistory(String pipelineName) throws PipelineManagerException {
    validatePipelineExistence(pipelineName);
    return stateTracker.getHistory(pipelineName);
  }

  public void deleteSnapshot(String pipelineName) {
    LOG.debug("Deleting snapshot");
    snapshotStore.deleteSnapshot(pipelineName);
    LOG.debug("Deleted snapshot");
  }

  public void deleteErrorRecords(String pipelineName, String rev, String stageInstanceName)
      throws PipelineStoreException {
    if(stageInstanceName == null || stageInstanceName.isEmpty()) {
      //delete error records for all stages in this pipeline
      LOG.debug("Deleting error records for pipeline {}", pipelineName);
      PipelineConfiguration pipelineConfiguration = pipelineStore.load(pipelineName, rev);
      for(StageConfiguration stageConf : pipelineConfiguration.getStages()) {
        errorRecordStore.deleteErrorRecords(pipelineName, rev, stageConf.getInstanceName());
      }
      LOG.debug("Deleted error records for pipeline {}", pipelineName);
    } else {
      LOG.debug("Deleting error records for stage {}", stageInstanceName);
      errorRecordStore.deleteErrorRecords(pipelineName, rev, stageInstanceName);
      LOG.debug("Deleted error records for stage {}", stageInstanceName);
    }
  }

  public PipelineState startPipeline(String name, String rev) throws PipelineStoreException
      , PipelineManagerException, PipelineRuntimeException, StageException {
    synchronized (pipelineMutex) {
      LOG.info("Starting pipeline {} {}", name, rev);
      validateStateTransition(State.RUNNING);
      return handleStartRequest(name, rev);
    }
  }

  public PipelineState stopPipeline() throws PipelineManagerException {
    synchronized (pipelineMutex) {
      validateStateTransition(State.STOPPING);
      setState(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING, Constants.STOP_PIPELINE_MESSAGE);
      PipelineState pipelineState = getPipelineState();
      handleStopRequest();
      return pipelineState;
    }
  }

  public MetricRegistry getMetrics() {
    return prodPipeline.getPipeline().getRunner().getMetrics();
  }



  private PipelineState handleStartRequest(String name, String rev) throws PipelineManagerException, StageException
      , PipelineRuntimeException, PipelineStoreException {

    prodPipeline = createProductionPipeline(name, rev, configuration, pipelineStore, stageLibrary);
    pipelineRunnable = new ProductionPipelineRunnable(this, prodPipeline, name, rev);
    executor.submit(pipelineRunnable);
    setState(name, rev, State.RUNNING, null);
    LOG.debug("Started pipeline {} {}", name, rev);
    return getPipelineState();
  }

  private void handleStopRequest() {
    LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
    if(pipelineRunnable != null) {
      pipelineRunnable.stop();
      pipelineRunnable = null;
    }
    LOG.debug("Stopped pipeline");
  }

  private ProductionPipeline createProductionPipeline(String name, String rev, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) throws PipelineStoreException
      , PipelineRuntimeException, StageException {

    //retrieve pipeline properties from the pipeline configuration
    int maxBatchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);
    //load pipeline configuration from store
    PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    for(ConfigConfiguration config : pipelineConfiguration.getConfiguration()) {
      if(Constants.DELIVERY_GUARANTEE.equals(config.getName())) {
        deliveryGuarantee = DeliveryGuarantee.valueOf((String)config.getValue());
      }
    }
    //create the pipeline directory eagerly.
    //This helps avoid race conditions when different stores attempt to create directories
    //Creating directory eagerly also avoids the need of synchronization
    createPipelineDirIfNotExist(name);

    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(name, runtimeInfo);
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore, errorRecordStore, offsetTracker,
        maxBatchSize, deliveryGuarantee, name, rev);
    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, pipelineConfiguration);

    return builder.build(runner);
  }

  @VisibleForTesting
  public StateTracker getStateTracker() {
    return stateTracker;
  }

  public void validateStateTransition(State toState) throws PipelineManagerException {
    State currentState = getPipelineState().getState();
    checkState(VALID_TRANSITIONS.get(currentState).contains(toState)
        , ContainerError.CONTAINER_0102, currentState, toState);
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

  private void createPipelineDirIfNotExist(String name) {
    File pipelineDir = new File(new File(runtimeInfo.getDataDir(), RUN_INFO_DIR), name);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
  }
}
