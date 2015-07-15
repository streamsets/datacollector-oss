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
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.dc.execution.runner.standalone.StandaloneRunner;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.callback.CallbackServerMetricsEventListener;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;

import javax.inject.Inject;
import java.util.List;


public class SlaveStandaloneRunner implements Runner {

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
  public void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException {
    standaloneRunner.start();
    if (standaloneRunner.getState().getStatus().isActive()) {
      String callbackServerURL = configuration.get(CALLBACK_SERVER_URL_KEY, CALLBACK_SERVER_URL_DEFAULT);
      String sdcClusterToken = configuration.get(SDC_CLUSTER_TOKEN_KEY, null);
      if(callbackServerURL != null) {
        addMetricsEventListener(new CallbackServerMetricsEventListener(runtimeInfo, callbackServerURL,
          sdcClusterToken));
      } else {
        throw new RuntimeException("No callback server URL is passed. SDC in Slave mode requires callback server URL (callback.server.url).");
      }
    }

  }

  @Override
  public String captureSnapshot(String name, int batches) throws PipelineRunnerException, PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Snapshot getSnapshot(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshot(String id) {
    throw new UnsupportedOperationException();
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
  public MetricRegistry getMetrics() {
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

}
