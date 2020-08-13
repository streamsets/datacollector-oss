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
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.util.EdgeUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EdgeRunner extends AbstractRunner implements StateListener {
  private static final Logger LOG = LoggerFactory.getLogger(EdgeRunner.class);

  private String pipelineTitle = null;

  public EdgeRunner(String name, String rev, ObjectGraph objectGraph) {
    super(name, rev);
    objectGraph.inject(this);
    PipelineBeanCreator.prepareForConnections(getConfiguration(), getRuntimeInfo());
  }

  @Override
  public String getPipelineTitle() {
    return pipelineTitle;
  }

  @Override
  public void resetOffset(String user) throws PipelineException {
    PipelineConfiguration pipelineConfiguration = getPipelineConfiguration(user);
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

    PipelineConfiguration pipelineConfiguration = getPipelineConfiguration(user);
    currentState = EdgeUtil.getEdgePipelineState(pipelineConfiguration);
    if (currentState != null && !currentState.getPipelineState().getStatus().isActive()) {
      LOG.warn("Pipeline {}:{} is already in stopped state {}",
          getName(),
          getRev(),
          currentState.getPipelineState().getStatus()
      );
      toState = currentState;
    } else {
      StartPipelineContext startPipelineContext = getStartPipelineContext();
      toState = EdgeUtil.stopEdgePipeline(
          pipelineConfiguration,
          startPipelineContext != null ? startPipelineContext.getRuntimeParameters() : null
      );
    }

    if (toState != null) {
      this.getPipelineStateStore().saveState(
          user,
          getName(),
          getRev(),
          BeanHelper.unwrapState(toState.getStatus()),
          toState.getMessage(),
          toState.getAttributes(),
          ExecutionMode.EDGE,
          toState.getMetrics(),
          toState.getRetryAttempt(),
          toState.getNextRetryTimeStamp()
      );
      getEventListenerManager().broadcastStateChange(
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
  public void prepareForStart(StartPipelineContext context) throws PipelineException {
    PipelineStateJson currentState;
    PipelineStateJson toState;

    setStartPipelineContext(context);
    PipelineConfiguration pipelineConfiguration = getPipelineConfiguration(context.getUser());
    currentState = EdgeUtil.getEdgePipelineState(pipelineConfiguration);
    if (currentState != null && currentState.getPipelineState().getStatus().isActive()) {
      LOG.warn("Pipeline {}:{} is already in active state {}",
          getName(),
          getRev(),
          currentState.getPipelineState().getStatus()
      );
      toState = currentState;
    } else {
      EdgeUtil.publishEdgePipeline(pipelineConfiguration, null);
      toState = EdgeUtil.startEdgePipeline(pipelineConfiguration, context.getRuntimeParameters());
    }

    if (toState != null) {
      this.getPipelineStateStore().saveState(
          context.getUser(),
          getName(),
          getRev(),
          BeanHelper.unwrapState(toState.getStatus()),
          toState.getMessage(),
          toState.getAttributes(),
          ExecutionMode.EDGE,
          toState.getMetrics(),
          toState.getRetryAttempt(),
          toState.getNextRetryTimeStamp()
      );
      getEventListenerManager().broadcastStateChange(
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
  public void start(StartPipelineContext context) throws PipelineException, StageException {
  }

  @Override
  public void startAndCaptureSnapshot(
      StartPipelineContext context,
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
  public Object getMetrics() throws PipelineException {
    PipelineConfiguration pipelineConfiguration = getPipelineConfiguration(null);
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
  public Map<String, Object> updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
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

  @Override
  public Runner getDelegatingRunner() {
    return null;
  }
}
