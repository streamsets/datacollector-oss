/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.execution.runner.edge;

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.EdgeUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EdgeRunner extends AbstractRunner implements StateListener {
  private static final Logger LOG = LoggerFactory.getLogger(EdgeRunner.class);
  @Inject PipelineStoreTask pipelineStoreTask;
  @Inject PipelineStateStore pipelineStateStore;

  private final String pipelineId;
  private String pipelineTitle = null;
  private final String rev;

  public EdgeRunner(String name, String rev, ObjectGraph objectGraph) {
    this.pipelineId = name;
    this.rev = rev;
    objectGraph.inject(this);
  }

  @Override
  public String getName() {
    return pipelineId;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public String getPipelineTitle() {
    return pipelineTitle;
  }

  @Override
  public void resetOffset(String user) throws PipelineException {
    PipelineConfiguration pipelineConfiguration = pipelineStoreTask.load(pipelineId, rev);
    EdgeUtil.resetOffset(pipelineConfiguration);
  }

  @Override
  public SourceOffset getCommittedOffsets() {
    return null;
  }

  @Override
  public void updateCommittedOffsets(SourceOffset sourceOffset) {

  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(pipelineId, rev);
  }

  @Override
  public void prepareForDataCollectorStart(String user) {

  }

  @Override
  public void onDataCollectorStart(String user) {

  }

  @Override
  public void onDataCollectorStop(String user) {

  }

  @Override
  public void stop(String user) throws PipelineException {
    PipelineStateJson currentState;
    PipelineStateJson toState;

    PipelineConfiguration pipelineConfiguration = pipelineStoreTask.load(pipelineId, rev);
    currentState = EdgeUtil.getEdgePipelineState(pipelineConfiguration);
    if (currentState != null && !currentState.getPipelineState().getStatus().isActive()) {
      LOG.warn("Pipeline {}:{} is already in stopped state {}",
          pipelineId,
          rev,
          currentState.getPipelineState().getStatus()
      );
      toState = currentState;
    } else {
      toState = EdgeUtil.stopEdgePipeline(pipelineConfiguration, runtimeParameters);
    }

    if (toState != null) {
      this.pipelineStateStore.saveState(
          user,
          pipelineId,
          rev,
          BeanHelper.unwrapState(toState.getStatus()),
          toState.getMessage(),
          toState.getAttributes(),
          ExecutionMode.EDGE,
          toState.getMetrics(),
          toState.getRetryAttempt(),
          toState.getNextRetryTimeStamp()
      );
      eventListenerManager.broadcastStateChange(
          currentState != null ? currentState.getPipelineState() : toState.getPipelineState(),
          toState.getPipelineState(),
          ThreadUsage.STANDALONE,
          null
      );
    }
  }

  @Override
  public void forceQuit(String user) {

  }

  @Override
  public void prepareForStart(String user, Map<String, Object> attributes) throws PipelineException {
    PipelineStateJson currentState;
    PipelineStateJson toState;

    PipelineConfiguration pipelineConfiguration = pipelineStoreTask.load(pipelineId, rev);
    currentState = EdgeUtil.getEdgePipelineState(pipelineConfiguration);
    if (currentState != null && currentState.getPipelineState().getStatus().isActive()) {
      LOG.warn("Pipeline {}:{} is already in active state {}",
          pipelineId,
          rev,
          currentState.getPipelineState().getStatus()
      );
      toState = currentState;
    } else {
      EdgeUtil.publishEdgePipeline(pipelineConfiguration);
      toState = EdgeUtil.startEdgePipeline(pipelineConfiguration, runtimeParameters);
    }

    if (toState != null) {
      this.pipelineStateStore.saveState(
          user,
          pipelineId,
          rev,
          BeanHelper.unwrapState(toState.getStatus()),
          toState.getMessage(),
          toState.getAttributes(),
          ExecutionMode.EDGE,
          toState.getMetrics(),
          toState.getRetryAttempt(),
          toState.getNextRetryTimeStamp()
      );
      eventListenerManager.broadcastStateChange(
          currentState != null ? currentState.getPipelineState() : toState.getPipelineState(),
          toState.getPipelineState(),
          ThreadUsage.STANDALONE,
          null
      );
    }
  }

  @Override
  public void prepareForStop(String user) {

  }

  @Override
  public void start(String user, Map<String, Object> runtimeParameters) {
    // We are Edge pipeline in prepareForStart call
  }

  @Override
  public void startAndCaptureSnapshot(
      String user,
      Map<String, Object> runtimeParameters,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) {

  }

  @Override
  public String captureSnapshot(
      String user,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) {
    return null;
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) {
    return null;
  }

  @Override
  public Snapshot getSnapshot(String id) {
    return null;
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    return null;
  }

  @Override
  public void deleteSnapshot(String id) {

  }

  @Override
  public List<PipelineState> getHistory() {
    return null;
  }

  @Override
  public void deleteHistory() {

  }

  @Override
  public Object getMetrics() throws PipelineException {
    PipelineConfiguration pipelineConfiguration = pipelineStoreTask.load(pipelineId, rev);
    return EdgeUtil.getEdgePipelineMetrics(pipelineConfiguration);
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) {
    return null;
  }

  @Override
  public List<ErrorMessage> getErrorMessages(
      String stage,
      int max
  ) {
    return null;
  }

  @Override
  public List<SampledRecord> getSampledRecords(
      String sampleId,
      int max
  ) {
    return null;
  }

  @Override
  public List<AlertInfo> getAlerts() {
    return null;
  }

  @Override
  public boolean deleteAlert(String alertId) {
    return false;
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {

  }

  @Override
  public Map getUpdateInfo() {
    return null;
  }

  @Override
  public String getToken() {
    return null;
  }

  @Override
  public int getRunnerCount() {
    return 0;
  }

  @Override
  public void stateChanged(
      PipelineStatus pipelineStatus,
      String message,
      Map<String, Object> attributes
  ) {
  }
}
