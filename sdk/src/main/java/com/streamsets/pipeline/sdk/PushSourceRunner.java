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
package com.streamsets.pipeline.sdk;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.*;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PushSourceRunner extends StageRunner<PushSource>  implements PushSourceContextDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(PushSourceRunner.class);

  /**
   * Interface that caller needs to implement to get produced data.
   */
  public interface Callback {
    public void processBatch(Output output);
  }

  public PushSourceRunner(
    Class<PushSource> stageClass,
    Map<String, Object> configuration,
    List<String> outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
  ) {
    super(
      stageClass,
      StageType.SOURCE,
      configuration,
      outputLanes,
      isPreview,
      onRecordError,
      constants,
      stageSdcConf,
      executionMode,
      deliveryGuarantee,
      resourcesDir,
      runtimeInfo,
      services
    );
  }

  public PushSourceRunner(
    Class<PushSource> stageClass,
    PushSource stage,
    Map<String, Object> configuration,
    List<String> outputLanes,
    boolean isPreview,
    OnRecordError onRecordError,
    Map<String, Object> constants,
    Map<String, String> stageSdcConf,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    String resourcesDir,
    RuntimeInfo runtimeInfo,
    List<ServiceRunner> services
  ) {
    super(
      stageClass,
      stage,
      StageType.SOURCE,
      configuration,
      outputLanes,
      isPreview,
      onRecordError,
      constants,
      stageSdcConf,
      executionMode,
      deliveryGuarantee,
      resourcesDir,
      runtimeInfo,
      services
    );
  }

  /**
   * Offsets as they are committed by the origin.
   */
  private Map<String, String> offsets;

  /**
   * Callback object that is called when a batch is available.
   */
  private Callback callback;

  /**
   * Internal executor service for running the origin in separate thread.
   */
  private ExecutorService executor;

  /**
   * Future where the origin is actually running.
   */
  private Future<Void> stageRunner;

  public void runProduce(Map<String, String> lastOffsets, int maxBatchSize, Callback callback) throws StageException {
    Preconditions.checkNotNull(lastOffsets, "Last offsets can't be null");
    Preconditions.checkNotNull(callback, "Callback object can't be null");

    this.offsets = new HashMap<>(lastOffsets);
    this.callback = callback;
    executor = Executors.newSingleThreadExecutor();

    try {
      LOG.debug("Stage '{}' produce starts", getInfo().getInstanceName());
      ensureStatus(Status.INITIALIZED);
      ((StageContext)getContext()).setPushSourceContextDelegate(this);
      stageRunner = (Future<Void>) executor.submit(new StageRunnable(lastOffsets, maxBatchSize));
    } finally {
      LOG.debug("Stage '{}' produce ends", getInfo().getInstanceName());
    }
  }

  /**
   * Runnable capable of running the origin's code.
   */
  private class StageRunnable implements Runnable {

    Map<String, String> lastOffsets;
    int maxBatchSize;

    StageRunnable(Map<String, String> lastOffsets, int maxBatchSize) {
      this.lastOffsets = lastOffsets;
      this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void run() {
      try {
        getStage().produce(lastOffsets, maxBatchSize);
      } catch (StageException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Graceful shutdown.
   *
   * Will only set flag to stop and return immediately.
   */
  public void setStop() {
    ((StageContext)getContext()).setStop(true);
  }

  /**
   * Wait on the origin's thread to finish.
   *
   * Caller code MUST call setStop(), otherwise this method will never return.
   */
  public void waitOnProduce() throws ExecutionException, InterruptedException {
    try {
      stageRunner.get();
    } finally {
      executor.shutdownNow();
    }
  }

  @Override
  public BatchContext startBatch() {
    BatchMakerImpl batchMaker = new BatchMakerImpl(((PushSource.Context) getContext()).getOutputLanes());
    return new BatchContextSdkImpl(batchMaker, (StageContext) getContext());
  }

  @Override
  public boolean processBatch(BatchContext batchContext, String entityName, String entityOffset) {
    callback.processBatch(StageRunner.getOutput(entityName, entityOffset, batchContext.getBatchMaker()));

    if(entityName != null) {
      commitOffset(entityName, entityOffset);
    }
    return true;
  }

  @Override
  public void commitOffset(String entityName, String entityOffset) {
    Preconditions.checkNotNull(entityName);
    if(entityOffset == null) {
      offsets.remove(entityName);
    } else {
      offsets.put(entityName, entityOffset);
    }
  }

  /**
   * Return offsets that were committed by the origin on last execution.
   */
  public Map<String, String> getOffsets() {
    return this.offsets;
  }

  public static class Builder extends StageRunner.Builder<PushSource, PushSourceRunner, Builder> {

    public Builder(Class<? extends PushSource> sourceClass,  PushSource source) {
      super((Class<PushSource>)sourceClass, source);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends PushSource> sourceClass) {
      super((Class<PushSource>) sourceClass);
    }

    @Override
    public PushSourceRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Source must have at least one output stream");
      if (stage != null) {
        return new PushSourceRunner(
          stageClass,
          stage,
          configs,
          outputLanes,
          isPreview,
          onRecordError,
          constants,
          stageSdcConf,
          executionMode,
          deliveryGuarantee,
          resourcesDir,
          runtimeInfo,
          services
        );
      } else {
        return new PushSourceRunner(
          stageClass,
          configs,
          outputLanes,
          isPreview,
          onRecordError,
          constants,
          stageSdcConf,
          executionMode,
          deliveryGuarantee,
          resourcesDir,
          runtimeInfo,
          services
        );
      }
    }

  }

}
