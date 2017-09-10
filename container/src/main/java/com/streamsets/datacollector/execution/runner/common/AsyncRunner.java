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
package com.streamsets.datacollector.execution.runner.common;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.execution.PipelineInfo;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
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
  private final SafeScheduledExecutorService runnerStopExecutor;

  @Inject
  public AsyncRunner(
    Runner runner,
    @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor,
    @Named("runnerStopExecutor") SafeScheduledExecutorService runnerStopExecutor
  ) {
    this.runner = runner;
    this.runnerExecutor = runnerExecutor;
    this.runnerStopExecutor = runnerStopExecutor;
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
  public String getPipelineTitle() throws PipelineException {
    return runner.getPipelineTitle();
  }

  @Override
  public void resetOffset(String user) throws PipelineException {
    runner.resetOffset(user);
  }

  @Override
  public SourceOffset getCommittedOffsets() throws PipelineException {
    return runner.getCommittedOffsets();
  }

  @Override
  public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    runner.updateCommittedOffsets(sourceOffset);
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return runner.getState();
  }

  @Override
  public void prepareForDataCollectorStart(String user) throws PipelineException {
    runner.prepareForDataCollectorStart(user);
  }

  @Override
  public void onDataCollectorStart(String user) throws PipelineException, StageException {
    Callable<Object> callable = () -> {
       runner.onDataCollectorStart(user);
       return null;
    };
    runnerExecutor.submit(callable);
  }

  @Override
  public void onDataCollectorStop(String user) throws PipelineException {
    runner.onDataCollectorStop(user);
  }

  @Override
  public void stop(String user) throws PipelineException {
    runner.prepareForStop(user);
    Callable<Object> callable = () -> {
      runner.stop(user);
      return null;
    };
    runnerStopExecutor.submit(callable);
  }

  @Override
  public void forceQuit(String user) throws PipelineException {
    if (getState().getStatus() != PipelineStatus.STOPPING){
      // Should not call force quit when pipeline is not stopping. No-op
      return;
    }
    Callable<Object> callable = () -> {
      runner.forceQuit(user);
      return null;
    };
    runnerStopExecutor.submit(callable);
  }

  @Override
  public void prepareForStart(String user) throws PipelineStoreException, PipelineRunnerException {
    throw new UnsupportedOperationException("This method is not supported for AsyncRunner. Call start() instead.");
  }

  @Override
  public void start(String user) throws PipelineException, StageException {
    start(user, null);
  }

  @Override
  public synchronized void start(
      String user,
      Map<String, Object> runtimeParameters
  ) throws PipelineException, StageException {
    runner.prepareForStart(user);
    Callable<Object> callable = () -> {
       runner.start(user, runtimeParameters);
       return null;
    };
    runnerExecutor.submit(callable);
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
    if(batchSize <= 0) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0107, batchSize);
    }
    runner.prepareForStart(user);
    Callable<Object> callable = () -> {
      runner.startAndCaptureSnapshot(user, runtimeParameters, snapshotName, snapshotLabel, batches, batchSize);
      return null;
    };
    runnerExecutor.submit(callable);
  }

  @Override
  public String captureSnapshot(String user, String name, String label, int batches, int batchSize) throws PipelineException {
    return runner.captureSnapshot(user, name, label, batches, batchSize);
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
    return runner.updateSnapshotLabel(snapshotName, snapshotLabel);
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
  public void deleteHistory() throws PipelineException {
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
  public boolean deleteAlert(String alertId) throws PipelineException {
    return runner.deleteAlert(alertId);
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineException {
    return runner.getAlerts();
  }

  @Override
  public void close() {
    runner.close();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return runner.getSlaveCallbackList(callbackObjectType);
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
  public int getRunnerCount() {
    return runner.getRunnerCount();
  }

  @Override
  public void prepareForStop(String user) {
    throw new UnsupportedOperationException("This method is not supported for AsyncRunner. Call stop() instead.");
  }
}
