/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

// 3 Runner implementations:
//  STANDALONE  : current standalone
//  CLUSTER: cluster streaming
//  SLAVE: cluster streaming
//  BATCH  : cluster batch
public interface Runner {

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
  public void onDataCollectorStart() throws PipelineException, StageException;

  // called for all existing pipelines when the data collector is shutting down
  // it should disconnect/reset-status of all pipelines
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException;

  // stops the pipeline
  public void stop() throws PipelineException;

  // Sets the state to STARTING. Should be called before doing a start on async runners.
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException;

  // Sets the state to STOPPING. Should be called before doing a stop on async runners.
  public void prepareForStop() throws PipelineStoreException, PipelineRunnerException;

  // starts the pipeline
  public void start() throws PipelineRunnerException, PipelineStoreException, PipelineRuntimeException, StageException;

  // triggers a snapshot request
  // delegates to SnapshotStore
  public String captureSnapshot(String name, int batches, int batchSize) throws PipelineException;

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

  Collection<CallbackInfo> getSlaveCallbackList();

  void close();

  void updateSlaveCallbackInfo(CallbackInfo callbackInfo);

  Map getUpdateInfo();

  String getToken();

}
