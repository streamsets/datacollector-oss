/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.execution.runner.common;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.execution.PipelineInfo;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class AsyncRunner implements Runner, PipelineInfo {

  private final Runner runner;
  private final SafeScheduledExecutorService runnerExecutor;

  @Inject
  public AsyncRunner (Runner runner, @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor) {
    this.runner = runner;
    this.runnerExecutor = runnerExecutor;
  }

  @Override
  public String getName() {
    return runner.getName();
  }

  @Override
  public String getRev() {
    return runner.getRev();
  }

  @Override
  public String getUser() {
    return runner.getUser();
  }

  @Override
  public void resetOffset() throws PipelineStoreException, PipelineRunnerException {
    runner.resetOffset();
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return runner.getState();
  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    runner.prepareForDataCollectorStart();
  }

  @Override
  public void onDataCollectorStart() throws PipelineException, StageException {
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineException, StageException {
         runner.onDataCollectorStart();
         return null;
      }
    };
    runnerExecutor.submit(callable);
  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    runner.onDataCollectorStop();
  }

  @Override
  public void stop() throws PipelineException {
    runner.prepareForStop();
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineException {
        runner.stop();
        return null;
      }
    };
    runnerExecutor.submit(callable);
  }

  @Override
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException {
    throw new UnsupportedOperationException("This method is not supported for AsyncRunner. Call start() instead.");
  }

  @Override
  public synchronized void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException {
    runner.prepareForStart();
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
         runner.start();
         return null;
      }
    };
    runnerExecutor.submit(callable);
  }

  @Override
  public String captureSnapshot(String name, int batches, int batchSize) throws PipelineException {
    return runner.captureSnapshot(name, batches, batchSize);
  }

  @Override
  public Snapshot getSnapshot(String id) throws PipelineException {
    return runner.getSnapshot(id);
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
    return runner.getSnapshotsInfo();
  }

  @Override
  public void deleteSnapshot(String id) throws PipelineException {
    runner.deleteSnapshot(id);
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return runner.getHistory();
  }

  @Override
  public void deleteHistory() {
    runner.deleteHistory();
  }

  @Override
  public Object getMetrics() throws PipelineStoreException {
    return runner.getMetrics();
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    return runner.getErrorRecords(stage, max);
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return runner.getErrorMessages(stage, max);
  }

  @Override
  public List<SampledRecord> getSampledRecords(String sampleId, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return runner.getSampledRecords(sampleId, max);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    return runner.deleteAlert(alertId);
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineStoreException {
    return runner.getAlerts();
  }

  @Override
  public void close() {
    runner.close();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    return runner.getSlaveCallbackList();
  }

  @Override
  public Pipeline getPipeline() {
    if (runner instanceof PipelineInfo) {
      return ((PipelineInfo) runner).getPipeline();
    } else {
      throw new UnsupportedOperationException(Utils.format("Runner '{}' does not support retrieval of  pipeline",
        runner.getClass().getName()));
    }
  }

  @VisibleForTesting
  public Runner getRunner() {
    return runner;
  }

  @Override
  public void updateSlaveCallbackInfo(com.streamsets.datacollector.callback.CallbackInfo callbackInfo) {
    runner.updateSlaveCallbackInfo(callbackInfo);
  }

  @Override
  public Map getUpdateInfo() {
    return runner.getUpdateInfo();
  }

  @Override
  public String getToken() {
    return runner.getToken();
  }

  @Override
  public void prepareForStop() {
    throw new UnsupportedOperationException("This method is not supported for AsyncRunner. Call stop() instead.");
  }
}
