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
package com.streamsets.datacollector.execution.runner.slave;

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.callback.CallbackServerErrorEventListener;
import com.streamsets.datacollector.callback.CallbackServerMetricsEventListener;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineInfo;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.MDC;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SlaveStandaloneRunner implements Runner, PipelineInfo  {

  private final StandaloneRunner standaloneRunner;
  private final Configuration configuration;
  private final RuntimeInfo runtimeInfo;
  private final EventListenerManager eventListenerManager;

  @Inject
  public SlaveStandaloneRunner(StandaloneRunner standaloneRunner, Configuration configuration, RuntimeInfo
    runtimeInfo, EventListenerManager eventListenerManager) {
    this.standaloneRunner = standaloneRunner;
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
    this.eventListenerManager = eventListenerManager;
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
  public Map<String, ConnectionConfiguration> getConnections() {
    return standaloneRunner.getConnections();
  }

  @Override
  public String getPipelineTitle() throws PipelineException {
    return standaloneRunner.getPipelineTitle();
  }

  @Override
  public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
    return standaloneRunner.getPipelineConfiguration(user);
  }

  @Override
  public void resetOffset(String user) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SourceOffset getCommittedOffsets() throws PipelineException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return standaloneRunner.getState();
  }

  @Override
  public void prepareForDataCollectorStart(String user) throws PipelineStoreException, PipelineRunnerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void onDataCollectorStart(String user) throws PipelineException, StageException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void onDataCollectorStop(String user) throws PipelineException {
    standaloneRunner.onDataCollectorStop(user);
  }

  @Override
  public void stop(String user) throws PipelineException {
    standaloneRunner.stop(user);
  }

  @Override
  public void forceQuit(String user) throws PipelineException {
    throw new UnsupportedOperationException("ForceQuit is not supported in Slave Standalone mode");
  }


  @Override
  public void prepareForStart(StartPipelineContext context) throws PipelineException {
    // no need for clear since slaves never run more than one pipeline
    MDC.put(LogConstants.USER, context.getUser());
    LogUtil.injectPipelineInMDC(getPipelineTitle(), getName());
    standaloneRunner.prepareForStart(context);
  }

  @Override
  public void start(StartPipelineContext context) throws PipelineException, StageException {
    String callbackServerURL = configuration.get(Constants.CALLBACK_SERVER_URL_KEY, Constants.CALLBACK_SERVER_URL_DEFAULT);
    String clusterToken = configuration.get(Constants.PIPELINE_CLUSTER_TOKEN_KEY, null);
    if (callbackServerURL != null) {
      eventListenerManager.addMetricsEventListener(this.getName(), new CallbackServerMetricsEventListener(context.getUser(),
        getName(), getRev(), runtimeInfo, callbackServerURL, clusterToken, standaloneRunner.getToken()));
      standaloneRunner.addErrorListener(new CallbackServerErrorEventListener(context.getUser(),
          getName(), getRev(), runtimeInfo, callbackServerURL, clusterToken, standaloneRunner.getToken()));
    } else {
      throw new RuntimeException(
        "No callback server URL is passed. SDC in Slave mode requires callback server URL (callback.server.url).");
    }
    standaloneRunner.start(context);
  }

  @Override
  public void startAndCaptureSnapshot(
      StartPipelineContext context,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException, StageException {
    standaloneRunner.captureSnapshot(context.getUser(), snapshotName, snapshotLabel, batches, batchSize);
  }

  @Override
  public String captureSnapshot(String user, String snapshotName, String snapshotLabel, int batches, int batchSize)
      throws PipelineException {
    return standaloneRunner.captureSnapshot(user, snapshotName, snapshotLabel, batches, batchSize);
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
    return standaloneRunner.updateSnapshotLabel(snapshotName, snapshotLabel);
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
    return standaloneRunner.getHistory();
  }

  @Override
  public void deleteHistory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getMetrics() throws PipelineStoreException {
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
  public List<SampledRecord> getSampledRecords(String sampleId, int max) throws PipelineRunnerException,
    PipelineStoreException {
    return standaloneRunner.getSampledRecords(sampleId, max);
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    return standaloneRunner.deleteAlert(alertId);
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineException {
    return standaloneRunner.getAlerts();
  }

  @Override
  public void close() {
    standaloneRunner.close();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return standaloneRunner.getSlaveCallbackList(callbackObjectType);
  }

  @Override
  public Map<String, Object> createStateAttributes() {
    return new HashMap<>();
  }

  @Override
  public Pipeline getPipeline() {
    return standaloneRunner.getPipeline();
  }

  @Override
  public Map<String, Object> updateSlaveCallbackInfo(com.streamsets.datacollector.callback.CallbackInfo callbackInfo) {
    return standaloneRunner.updateSlaveCallbackInfo(callbackInfo);
  }

  @Override
  public String getToken() {
    return standaloneRunner.getToken();
  }

  @Override
  public int getRunnerCount() {
    return standaloneRunner.getRunnerCount();
  }

  @Override
  public void prepareForStop(String user) throws PipelineException {
    // no need for clear since slaves never run more than one pipeline
    MDC.put(LogConstants.USER, user);
    LogUtil.injectPipelineInMDC(getPipelineTitle(), getName());
    standaloneRunner.prepareForStop(user);
  }

  @Override
  public Runner getDelegatingRunner() {
    return standaloneRunner;
  }
}
