/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dc.execution.alerts.AlertInfo;
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.dc.callback.CallbackInfo;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.PipelineException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

// 3 Runner implementations:
//  STANDALONE  : current standalone
//  CLUSTER: cluster streaming
//  SLAVE: cluster streaming
//  BATCH  : cluster batch
public interface Runner {
  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;
  public static final String CALLBACK_SERVER_URL_KEY = "callback.server.url";
  public static final String CALLBACK_SERVER_URL_DEFAULT = null;
  public static final String SDC_CLUSTER_TOKEN_KEY = "sdc.cluster.token";

  //Runners are lightweight control classes, they are created on every Manager.getRunner() call

  //ALl impls receive a PipelineStore instance at <init> time, to load the PipelineConfiguration if necessary
  //All impls receive a StateStore instance at <init> time.
  //All impls receive a SnapshotStore instance at <init> time.
  //All impls receive a ErrorStore instance at <init> time.
  //All impls must register all PipelineListeners (the above instance may or may not implement this interface)
  //    and dispatch pipeline start/stop calls to them.

  //each Runner has its own status transition rules

  // pipeline name
  public String getName();

  // pipeline revision
  public String getRev();

  // user that is operating the runner
  public String getUser();

  // resets the pipeline offset, only if the pipeline is not running
  // it must assert the current status
  public void resetOffset() throws PipelineStoreException, PipelineRunnerException;

  // pipeline status
  public PipelineState getState() throws PipelineStoreException;

  // called on startup, moves runner to disconnected state if necessary
  void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException;

  // called for all existing pipelines when the data collector starts
  // it should reconnect/reset-status of all pipelines
  // returns whether to start the pipeline on sdc start
  public void onDataCollectorStart() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException;

  // called for all existing pipelines when the data collector is shutting down
  // it should disconnect/reset-status of all pipelines
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException;

  // stops the pipeline
  public void stop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException;

  // Sets the state to STARTING. Should be called before doing a start on async runners.
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException;

  // starts the pipeline
  public void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException;

  // triggers a snapshot request
  // delegates to SnapshotStore
  public String captureSnapshot(String name, int batches, int batchSize) throws PipelineException, PipelineStoreException;

  // retrieves a snapshot base on its ID
  // delegates to SnapshotStore
  public Snapshot getSnapshot(String id) throws PipelineException;

  // lists all available snapshots
  // delegates to SnapshotStore
  public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException;

  // deletes a snapshot
  // delegates to SnapshotStore
  public void deleteSnapshot(String id) throws PipelineException;

  // the pipeline history
  // delegates to the the PipelineStateStore
  public List<PipelineState> getHistory() throws PipelineStoreException;

  public void deleteHistory();

  // gets the current pipeline metrics
  public Object getMetrics();

  // returns error records for a give stage
  // delegates to the ErrorStore
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException;

  // returns pipeline error for a give stage
  // delegates to the ErrorStore
  public List<ErrorMessage> getErrorMessages(String stage, int max) throws PipelineRunnerException, PipelineStoreException;

  public List<Record> getSampledRecords(String sampleId, int max) throws PipelineRunnerException, PipelineStoreException;

  public List<AlertInfo> getAlerts() throws PipelineStoreException;

  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException;

  void addStateEventListener(StateEventListener stateEventListener);

  void removeStateEventListener(StateEventListener stateEventListener);

  void addAlertEventListener(AlertEventListener alertEventListener);

  void removeAlertEventListener(AlertEventListener alertEventListener);

  void addMetricsEventListener(MetricsEventListener metricsEventListener);

  void removeMetricsEventListener(MetricsEventListener metricsEventListener);

  Collection<CallbackInfo> getSlaveCallbackList();

  void close();

  void updateSlaveCallbackInfo(CallbackInfo callbackInfo);

  Map getUpdateInfo();

}
