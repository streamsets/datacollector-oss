/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.List;

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
  public void resetOffset();

  // pipeline status
  public PipelineStatus getStatus();

  // called for all existing pipelines when the data collector starts
  // it should reconnect/reset-status of all pipelines
  public void onDataCollectorStart();

  // called for all existing pipelines when the data collector is shutting down
  // it should disconnect/reset-status of all pipelines
  public void onDataCollectorStop();

  // stops the pipeline
  public void stop();

  // starts the pipeline
  public void start();

  // triggers a snapshot request
  // delegates to SnapshotStore
  public String captureSnapshot(String name, int batches);

  // retrieves a snapshot base on its ID
  // delegates to SnapshotStore
  public Snapshot getSnapshot(String id);

  // lists all available snapshots
  // delegates to SnapshotStore
  public List<SnapshotInfo> getSnapshotsInfo();

  // deletes a snapshot
  // delegates to SnapshotStore
  public void deleteSnapshot(String id);

  // the pipeline history
  // delegates to the the PipelineStateStore
  public List<PipelineState> getHistory();

  // gets the current pipeline metrics
  public MetricRegistry getMetric();

  // returns error records for a give stage
  // delegates to the ErrorStore
  public List<Record> getErrorRecords(String stage, int max);

  // returns pipeline error for a give stage
  // delegates to the ErrorStore
  public List<ErrorMessage> getErrorMessages(String stage, int max);

  public List<Record> getSampledRecords(String sampleId, int max);

  public void deleteAlert(String alertId);

}
