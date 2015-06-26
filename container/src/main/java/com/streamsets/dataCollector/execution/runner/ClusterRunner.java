/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.Snapshot;
import com.streamsets.dataCollector.execution.SnapshotInfo;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.store.PipelineStoreException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
public class ClusterRunner implements Runner {

  private static final Map<PipelineStatus, Set<PipelineStatus>> VALID_TRANSITIONS =
    new ImmutableMap.Builder<PipelineStatus, Set<PipelineStatus>>()
    .put(PipelineStatus.EDITED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STARTING, ImmutableSet.of(PipelineStatus.START_ERROR, PipelineStatus.RUNNING, PipelineStatus.DISCONNECTING))
    .put(PipelineStatus.START_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    // cannot transition to disconnecting from Running
    .put(PipelineStatus.RUNNING, ImmutableSet.of(PipelineStatus.FINISHED, PipelineStatus.STOPPING, PipelineStatus.KILLED))
    .put(PipelineStatus.RUN_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPING, ImmutableSet.of(PipelineStatus.STOPPED))
    .put(PipelineStatus.FINISHED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.KILLED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.DISCONNECTING, ImmutableSet.of(PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.DISCONNECTED, ImmutableSet.of(PipelineStatus.CONNECTING))
    .put(PipelineStatus.CONNECTING, ImmutableSet.of(PipelineStatus.STARTING, PipelineStatus.RUNNING, PipelineStatus.CONNECT_ERROR))
    .build();

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getRev() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getUser() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void resetOffset() {
    // TODO Auto-generated method stub

  }

  @Override
  public PipelineStatus getStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void onDataCollectorStart() {
    // TODO Auto-generated method stub
 }

  @Override
  public void onDataCollectorStop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  @Override
  public void start() {
    // TODO Auto-generated method stub

  }

  @Override
  public String captureSnapshot(String name, int batches) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Snapshot getSnapshot(String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteSnapshot(String id) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<PipelineState> getHistory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MetricRegistry getMetrics() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Record> getSampledRecords(String sampleId, int max) {
    // TODO Auto-generated method stub
    return null;
  }

  public Collection<CallbackInfo> getSlaveCallbackList() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void broadcastAlerts(RuleDefinition ruleDefinition) {

  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
