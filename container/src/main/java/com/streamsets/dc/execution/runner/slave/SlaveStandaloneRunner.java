/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.slave;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dc.execution.PipelineState;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.Snapshot;
import com.streamsets.dc.execution.SnapshotInfo;
import com.streamsets.dc.execution.runner.common.PipelineInfo;
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.dc.execution.runner.standalone.StandaloneRunner;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.dc.callback.CallbackInfo;
import com.streamsets.dc.callback.CallbackServerMetricsEventListener;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.PipelineException;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public class SlaveStandaloneRunner implements Runner, PipelineInfo  {

  private final StandaloneRunner standaloneRunner;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public SlaveStandaloneRunner(StandaloneRunner standaloneRunner, Configuration configuration, RuntimeInfo
    runtimeInfo) {
    this.standaloneRunner = standaloneRunner;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public String getName() {
    return standaloneRunner.getName();
  }

  @Override
  public String getRev() {
    return standaloneRunner.getRev();
  }

  @Override
  public String getUser() {
    return standaloneRunner.getUser();
  }

  @Override
  public void resetOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return standaloneRunner.getState();
  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void onDataCollectorStart() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException,
    StageException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException {
    standaloneRunner.onDataCollectorStop();
  }

  @Override
  public void stop() throws PipelineStoreException, PipelineRunnerException {
    standaloneRunner.stop();
  }

  @Override
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException {
    standaloneRunner.prepareForStart();
  }

  @Override
  public void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException {
    String callbackServerURL = configuration.get(CALLBACK_SERVER_URL_KEY, CALLBACK_SERVER_URL_DEFAULT);
    String sdcClusterToken = configuration.get(SDC_CLUSTER_TOKEN_KEY, null);
    if (callbackServerURL != null) {
      addMetricsEventListener(new CallbackServerMetricsEventListener(getUser(), getName(), getRev(), runtimeInfo,
        callbackServerURL, sdcClusterToken));
    } else {
      throw new RuntimeException(
        "No callback server URL is passed. SDC in Slave mode requires callback server URL (callback.server.url).");
    }
    standaloneRunner.start();
  }

  @Override
  public String captureSnapshot(String snapshotName, int batches, int batchSize) throws PipelineException {
    return standaloneRunner.captureSnapshot(snapshotName, batches, batchSize);
  }

  @Override
  public Snapshot getSnapshot(String id) throws PipelineException {
    return standaloneRunner.getSnapshot(id);
  }
  @Override
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
    return standaloneRunner.getSnapshotsInfo();
  }

  @Override
  public void deleteSnapshot(String id) throws PipelineException {
    standaloneRunner.deleteSnapshot(id);
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteHistory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getMetrics() {
    return standaloneRunner.getMetrics();
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    return standaloneRunner.getErrorRecords(stage, max);
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return standaloneRunner.getErrorMessages(stage, max);
  }

  @Override
  public List<Record> getSampledRecords(String sampleId, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return standaloneRunner.getSampledRecords(sampleId, max);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    return standaloneRunner.deleteAlert(alertId);
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    standaloneRunner.addAlertEventListener(alertEventListener);
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    standaloneRunner.removeAlertEventListener(alertEventListener);
  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {
    standaloneRunner.broadcastAlerts(ruleDefinition);
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    standaloneRunner.addMetricsEventListener(metricsEventListener);
  }

  @Override
  public void close() {
    standaloneRunner.close();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    return standaloneRunner.getSlaveCallbackList();
  }

  @Override
  public Pipeline getPipeline() {
    return standaloneRunner.getPipeline();
  }

  @Override
  public void updateSlaveCallbackInfo(com.streamsets.dc.callback.CallbackInfo callbackInfo) {
    standaloneRunner.updateSlaveCallbackInfo(callbackInfo);
  }

  @Override
  public Map getUpdateInfo() {
    return standaloneRunner.getUpdateInfo();
  }


}
