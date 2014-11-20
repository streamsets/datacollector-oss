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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.task.AbstractTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.*;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class ProductionPipelineManagerTask extends AbstractTask {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineManagerTask.class);
  private static final String PRODUCTION_PIPELINE_MANAGER = "productionPipelineManager";
  private static final String PRODUCTION_PIPELINE_RUNNER = "ProductionPipelineRunner";

  private static final Map<State, Set<State>> VALID_TRANSITIONS = ImmutableMap.of(
      State.NOT_RUNNING, (Set<State>) ImmutableSet.of(State.RUNNING),
      State.RUNNING, (Set<State>) ImmutableSet.of(State.STOPPING),
      State.STOPPING, (Set<State>) ImmutableSet.of(State.STOPPING /*Try stopping many times, this should be no-op*/
          , State.NOT_RUNNING),
      State.ERROR, (Set<State>) ImmutableSet.of(State.RUNNING, State.NOT_RUNNING));

  private final RuntimeInfo runtimeInfo;
  private final ProductionSourceOffsetTracker offsetTracker;
  private final StateTracker stateTracker;
  private final SnapshotStore snapshotStore;
  private final Configuration configuration;
  private final PipelineStoreTask pipelineStore;
  private final StageLibraryTask stageLibrary;

  /*References the thread that is executing the pipeline currently */
  private ProductionPipelineRunnable pipelineRunnable;
  /*The executor service that is currently executing the ProdPipelineRunnerThread*/
  private ExecutorService executor;
  /*The pipeline being executed*/
  private ProductionPipeline prodPipeline;

  /*Mutex objects to synchronize start and stop pipeline methods*/
  private final Object pipelineMutex = new Object();

  @Inject
  public ProductionPipelineManagerTask(RuntimeInfo runtimeInfo, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) {
    super(PRODUCTION_PIPELINE_MANAGER);
    this.runtimeInfo = runtimeInfo;
    stateTracker = new StateTracker(runtimeInfo);
    offsetTracker = new ProductionSourceOffsetTracker(this.runtimeInfo);
    snapshotStore = new FileSnapshotStore(this.runtimeInfo);
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
  }


  public PipelineState getPipelineState() {
    return stateTracker.getState();
  }

  public void setState(String name, String rev, State state, String message) throws PipelineStateException {
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
      } catch (PipelineStateException e) {
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

  public String setOffset(String offset) throws PipelineStateException {
    LOG.debug("Setting offset {}", offset);
    //cannot set offset if current state is RUNNING
    checkState(!getPipelineState().getState().equals(State.RUNNING),
        PipelineStateException.ERROR.CANNOT_SET_OFFSET_RUNNING_STATE);
    offsetTracker.setOffset(offset);
    offsetTracker.commitOffset();
    return offsetTracker.getOffset();
  }

  public void captureSnapshot(int batchSize) throws PipelineStateException {
    LOG.debug("Capturing snapshot with batch size {}", batchSize);
    checkState(getPipelineState().getState().equals(State.RUNNING),
        PipelineStateException.ERROR.CANNOT_CAPTURE_SNAPSHOT_WHEN_PIPELINE_NOT_RUNNING);
    if(batchSize <= 0) {
      throw new PipelineStateException(PipelineStateException.ERROR.INVALID_BATCH_SIZE, batchSize);
    }
    prodPipeline.captureSnapshot(batchSize);
    LOG.debug("Captured snapshot with batch size {}", batchSize);
  }

  public SnapshotStatus getSnapshotStatus() {
    return snapshotStore.getSnapshotStatus();
  }

  public InputStream getSnapshot() {
    return snapshotStore.getSnapshot();
  }

  public void deleteSnapshot() {
    LOG.debug("Deleting snapshot");
    snapshotStore.deleteSnapshot();
    LOG.debug("Deleted snapshot");
  }

  public PipelineState startPipeline(String name, String rev) throws PipelineStoreException
      , PipelineStateException, PipelineRuntimeException, StageException {
    synchronized (pipelineMutex) {
      LOG.info("Starting pipeline {} {}", name, rev);
      validateStateTransition(State.RUNNING);
      return handleStartRequest(name, rev);
    }
  }

  public PipelineState stopPipeline() throws PipelineStateException {
    synchronized (pipelineMutex) {
      validateStateTransition(State.STOPPING);
      setState(pipelineRunnable.getName(), pipelineRunnable.getRev(), State.STOPPING, Constants.STOP_PIPELINE_MESSAGE);
      LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
      return handleStopRequest();
    }
  }

  private PipelineState handleStartRequest(String name, String rev) throws PipelineStateException, StageException
      , PipelineRuntimeException, PipelineStoreException {

    prodPipeline = createProductionPipeline(name, rev, configuration, pipelineStore, stageLibrary);
    pipelineRunnable = new ProductionPipelineRunnable(this, prodPipeline, name, rev);
    executor.submit(pipelineRunnable);
    setState(name, rev, State.RUNNING, null);
    LOG.debug("Started pipeline {} {}", name, rev);
    return getPipelineState();
  }

  private PipelineState handleStopRequest() {
    if(pipelineRunnable != null) {
      pipelineRunnable.stop();
      pipelineRunnable = null;
    }
    LOG.debug("Stopped pipeline");
    return getPipelineState();
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
    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotStore,offsetTracker, maxBatchSize
        , deliveryGuarantee);
    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(stageLibrary, name, pipelineConfiguration);

    return builder.build(runner);
  }

  @VisibleForTesting
  public StateTracker getStateTracker() {
    return stateTracker;
  }

  @VisibleForTesting
  ProductionSourceOffsetTracker getOffsetTracker() {return offsetTracker;}

  public void validateStateTransition(State toState) throws PipelineStateException {
    State currentState = getPipelineState().getState();
    checkState(VALID_TRANSITIONS.get(currentState).contains(toState)
        , PipelineStateException.ERROR.INVALID_STATE_TRANSITION, currentState, toState);
  }

  private void checkState(boolean expr, PipelineStateException.ERROR error, Object... args)
      throws PipelineStateException {
    if(!expr) {
      throw new PipelineStateException(error, args);
    }
  }

}
