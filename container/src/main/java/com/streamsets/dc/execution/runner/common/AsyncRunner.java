/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.common;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.dc.execution.PipelineState;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.Snapshot;
import com.streamsets.dc.execution.SnapshotInfo;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.dc.callback.CallbackInfo;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.PipelineException;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

public class AsyncRunner implements Runner, PipelineInfo {

  private final Runner runner;
  private final SafeScheduledExecutorService asyncExecutor;

  @Inject
  public AsyncRunner (Runner runner, @Named("asyncExecutor") SafeScheduledExecutorService asyncExecutor) {
    this.runner = runner;
    this.asyncExecutor = asyncExecutor;
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
  public void onDataCollectorStart() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException,
    StageException {
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
         runner.onDataCollectorStart();
         return null;
      }
    };
    asyncExecutor.submit(callable);
  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException {
    runner.onDataCollectorStop();
  }

  @Override
  public synchronized void stop() throws PipelineStoreException, PipelineRunnerException {
    runner.stop();
  }

  @Override
  public synchronized void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException {
    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
         runner.start();
         return null;
      }
    };
    asyncExecutor.submit(callable);
  }

  @Override
  public String captureSnapshot(String name, int batches) throws PipelineException, PipelineStoreException {
    return runner.captureSnapshot(name, batches);
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
  public Object getMetrics() {
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
  public List<Record> getSampledRecords(String sampleId, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return runner.getSampledRecords(sampleId, max);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    return runner.deleteAlert(alertId);
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    runner.addAlertEventListener(alertEventListener);
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    runner.removeAlertEventListener(alertEventListener);
  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    runner.broadcastAlerts(ruleDefinition);
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    runner.addMetricsEventListener(metricsEventListener);
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
      throw new UnsupportedOperationException(Utils.format("Runner '{}' doesnt support retrieval of "
        + "pipeline", runner.getClass().getName()));
    }
  }

  @VisibleForTesting
  public Runner getRunner() {
    return runner;
  }

  @Override
  public void updateSlaveCallbackInfo(com.streamsets.dc.callback.CallbackInfo callbackInfo) {
    runner.updateSlaveCallbackInfo(callbackInfo);
  }
}
