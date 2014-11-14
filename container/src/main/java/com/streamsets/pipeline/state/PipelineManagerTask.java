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
package com.streamsets.pipeline.state;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class PipelineManagerTask extends AbstractTask {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineManagerTask.class);

  private static final String MAX_BATCH_SIZE_KEY = "maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String DELIVERY_GUARANTEE = "deliveryGuarantee";
  private static final String DG_AT_LEAST_ONCE = "AT_LEAST_ONCE";

  private final RuntimeInfo runtimeInfo;
  private final ProductionSourceOffsetTracker offsetTracker;
  private final StateTracker stateTracker;
  private final SnapshotPersister snapshotPersister;

  /*References the thread that is executing the pipeline currently */
  private ProductionPipelineRunnable pipelineRunnable;
  /*The executor service that is currently executing the ProdPipelineRunnerThread*/
  private ExecutorService e;
  /*The pipeline being executed*/
  private ProductionPipeline prodPipeline;

  @Inject
  public PipelineManagerTask(RuntimeInfo runtimeInfo) {
    super("pipelineManager");
    this.runtimeInfo = runtimeInfo;
    stateTracker = new StateTracker(runtimeInfo);
    offsetTracker = new ProductionSourceOffsetTracker(this.runtimeInfo);
    snapshotPersister = new SnapshotPersister(this.runtimeInfo);
  }


  public PipelineState getState() {
    return stateTracker.getState();
  }

  public void setState(State state, String message) throws PipelineStateException {
    stateTracker.setState(state, message);
  }

  @Override
  public void initTask() {
    stateTracker.init();
  }

  @Override
  public void stopTask() {
    stopPipeline();
    if(e != null) {
      e.shutdown();
    }
  }

  public void stopPipeline() {
    if(pipelineRunnable != null) {
      pipelineRunnable.stop();
      pipelineRunnable = null;
    }
  }

  public String setOffset(String offset) throws PipelineStateException {
    //cannot set offset if current state is RUNNING
    if(getState().getPipelineState() == State.RUNNING) {
      throw new PipelineStateException(PipelineStateErrors.CANNOT_SET_OFFSET_RUNNING_STATE);
    }
    offsetTracker.setOffset(offset);
    offsetTracker.commitOffset();
    return offsetTracker.getOffset();
  }

  public void captureSnapshot(int batchSize) throws PipelineStateException {
    if(!getState().getPipelineState().equals(State.RUNNING)) {
      throw new PipelineStateException(PipelineStateErrors.CANNOT_CAPTURE_SNAPSHOT_WHEN_PIPELINE_NOT_RUNNING);
    }
    if(batchSize <= 0) {
      throw new PipelineStateException(PipelineStateErrors.INVALID_BATCH_SIZE, batchSize);
    }
    prodPipeline.captureSnapshot(batchSize);
  }

  public InputStream getSnapshot() {
    if(snapshotPersister.getSnapshotFile().exists()) {
      try {
        return new FileInputStream(snapshotPersister.getSnapshotFile());
      } catch (FileNotFoundException e) {
        LOG.warn(e.getMessage());
        return null;
      }
    }
    return null;
  }

  public void deleteSnapshot() {
    if(snapshotPersister.getSnapshotFile().exists()) {
      snapshotPersister.getSnapshotFile().delete();
    }
  }

  public PipelineState handleStateTransition(String name, String rev, State state
      , Configuration configuration, PipelineStoreTask pipelineStore, StageLibraryTask stageLib)
      throws PipelineStateException, StageException, PipelineRuntimeException, PipelineStoreException {

    State currentState = getState().getPipelineState();
    if (!currentState.isValidTransition(state.name())) {
      //current state does not support transition into the requested state
      throw new PipelineStateException(
          PipelineStateErrors.INVALID_STATE_TRANSITION, currentState, state);
    }

    //Allowed states
    //RUNNING -> NOT_RUNNING

    //RUNNING ->ERROR is not supported through the Rest API.
    //This can happen only when there is an error while executing the pipeline

    //ERROR -> NOT_RUNNING happens when the client has acknowledged
    // the error and created another pipeline.
    if (currentState == State.RUNNING) {
      if (state == State.NOT_RUNNING) {
        return handleStopRequest();
      }
    } else if (currentState == State.NOT_RUNNING) {
      if (state == State.RUNNING) {
        return handleStartRequest(name, rev, configuration, pipelineStore, stageLib);
      }
    } else if (currentState == State.ERROR) {
      if (state == State.NOT_RUNNING) {
        return handleErrorResetRequest();
      } else if (state == State.RUNNING) {
        return handleStartRequest(name, rev, configuration, pipelineStore, stageLib);
      }
    }

    throw new IllegalStateException("Unexpected state.");

  }

  private PipelineState handleErrorResetRequest() throws PipelineStateException {
    //this means that the client has acknowledged the error
    setState(State.NOT_RUNNING, null);
    return getState();
  }

  private synchronized PipelineState handleStartRequest(String name, String rev, Configuration configuration
      , PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary) throws PipelineStateException, StageException
      , PipelineRuntimeException, PipelineStoreException {

    prodPipeline = createProductionPipeline(name, rev, configuration, pipelineStore, stageLibrary);
    setState(State.RUNNING, null);
    pipelineRunnable = new ProductionPipelineRunnable(this, prodPipeline);
    e = Executors.newSingleThreadExecutor(new NamedThreadFactory("ProductionPipelineRunner"));
    e.submit(pipelineRunnable);
    return getState();
  }

  private PipelineState handleStopRequest() throws PipelineStateException {
    setState(State.NOT_RUNNING, null);
    stopPipeline();
    return getState();
  }

  private ProductionPipeline createProductionPipeline(
      String name, String rev, Configuration configuration, PipelineStoreTask pipelineStore, StageLibraryTask stageLibrary)
      throws PipelineStoreException, PipelineRuntimeException, StageException {

    //retrieve pipeline properties from the pipeline configuration
    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.valueOf(
        configuration.get(DELIVERY_GUARANTEE, DG_AT_LEAST_ONCE));
    PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);

    ProductionPipelineRunner runner = new ProductionPipelineRunner(snapshotPersister,
        offsetTracker, maxBatchSize, deliveryGuarantee);

    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(
        stageLibrary, name, pipelineConfiguration);

    return builder.build(runner);
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }

  @VisibleForTesting
  public StateTracker getStateTracker() {
    return stateTracker;
  }

  public ProductionSourceOffsetTracker getOffsetTracker() {
    return offsetTracker;
  }

  public SnapshotStatus snapshotStatus() {
    boolean snapshotFileExists = snapshotPersister.getSnapshotFile().exists();
    boolean snapshotStageFileExists = snapshotPersister.getSnapshotStageFile().exists();

    if(snapshotFileExists) {
      if(snapshotStageFileExists) {
        return new SnapshotStatus(true, SnapshotState.RUNNING);
      } else {
        return new SnapshotStatus(true, SnapshotState.NOT_RUNNING);
      }
    } else {
      if(snapshotStageFileExists) {
        return new SnapshotStatus(false, SnapshotState.RUNNING);
      } else {
        return new SnapshotStatus(false, SnapshotState.NOT_RUNNING);
      }
    }
  }

  class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private volatile int counter;

    public NamedThreadFactory(String baseName) {
      this.name = baseName + "-";
    }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = Executors.defaultThreadFactory().newThread(runnable);
      thread.setName(name + (counter++));
      thread.setDaemon(true);
      return thread;
    }
  }
}
