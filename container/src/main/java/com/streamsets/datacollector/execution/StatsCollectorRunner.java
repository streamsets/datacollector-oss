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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class StatsCollectorRunner implements Runner {
  private static final Logger LOG = LoggerFactory.getLogger(StatsCollectorRunner.class);

  private final Runner runner;
  private final StatsCollector statsCollector;

  public StatsCollectorRunner(Runner runner, StatsCollector statsCollector) {
    this.runner = runner;
    this.statsCollector = statsCollector;
  }

  public StatsCollector getStatsCollector() {
    return statsCollector;
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
  public Map<String, ConnectionConfiguration> getConnections() {
    return runner.getConnections();
  }

  @Override
  public String getPipelineTitle() throws PipelineException {
    return runner.getPipelineTitle();
  }

  @Override
  public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
    return runner.getPipelineConfiguration(user);
  }

  @Override
  public void resetOffset(String user) throws PipelineException {
    runner.resetOffset(user);
  }

  @Override
  public SourceOffset getCommittedOffsets() throws PipelineException {
    return runner.getCommittedOffsets();
  }

  @Override
  public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    runner.updateCommittedOffsets(sourceOffset);
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return runner.getState();
  }

  @Override
  public void prepareForDataCollectorStart(String user) throws PipelineException {
    runner.prepareForDataCollectorStart(user);
  }

  @Override
  public void onDataCollectorStart(String user) throws PipelineException, StageException {
    runner.onDataCollectorStart(user);
    try {
      statsCollector.startPipeline(getPipelineConfiguration(user));
    } catch (PipelineException e) {
      LOG.error("Can't start anonymous stats collection for pipeline.");
    }
  }

  @Override
  public void onDataCollectorStop(String user) throws PipelineException {
    runner.onDataCollectorStop(user);
    try {
      statsCollector.stopPipeline(getPipelineConfiguration(user));
    } catch (PipelineException e) {
      LOG.error("Can't stop anonymous stats collection for pipeline.");
    }
  }

  @Override
  public void stop(String user) throws PipelineException {
    runner.stop(user);
    try {
      statsCollector.stopPipeline(getPipelineConfiguration(user));
    } catch (PipelineException e) {
      LOG.error("Can't stop anonymous stats collection for pipeline.");
    }
  }

  @Override
  public void forceQuit(String user) throws PipelineException {
    runner.forceQuit(user);
    try {
      statsCollector.stopPipeline(getPipelineConfiguration(user));
    } catch (PipelineException e) {
      LOG.error("Can't force stop anonymous stats collection for pipeline.");
    }
  }

  @Override
  public void prepareForStart(StartPipelineContext context) throws PipelineException {
    runner.prepareForStart(context);
  }

  @Override
  public void prepareForStop(String user) throws PipelineException {
    runner.prepareForStop(user);
  }

  @Override
  public void start(StartPipelineContext context) throws PipelineException, StageException {
    runner.start(context);
    try {
      statsCollector.startPipeline(getPipelineConfiguration(context.getUser()));
    } catch (PipelineException e) {
      LOG.error("Can't start anonymous stats collection for pipeline.");
    }
  }

  @Override
  public void startAndCaptureSnapshot(
      StartPipelineContext context,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException, StageException {
    runner.startAndCaptureSnapshot(context, snapshotName, snapshotLabel, batches, batchSize);
    try {
      statsCollector.startPipeline(getPipelineConfiguration(context.getUser()));
    } catch (PipelineException e) {
      LOG.error("Can't start anonymous stats collection for pipeline.");
    }
  }

  @Override
  public String captureSnapshot(String user, String snapshotName, String snapshotLabel, int batches, int batchSize)
      throws PipelineException {
    return runner.captureSnapshot(user, snapshotName, snapshotLabel, batches, batchSize);
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
    return runner.updateSnapshotLabel(snapshotName, snapshotLabel);
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
  public void deleteHistory() throws PipelineException {
    runner.deleteHistory();
  }

  @Override
  public Object getMetrics() throws PipelineException {
    return runner.getMetrics();
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    return runner.getErrorRecords(stage, max);
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max)
      throws PipelineRunnerException, PipelineStoreException {
    return runner.getErrorMessages(stage, max);
  }

  @Override
  public List<SampledRecord> getSampledRecords(
      String sampleId, int max
  ) throws PipelineRunnerException, PipelineStoreException {
    return runner.getSampledRecords(sampleId, max);
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineException {
    return runner.getAlerts();
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineException {
    return runner.deleteAlert(alertId);
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return runner.getSlaveCallbackList(callbackObjectType);
  }

  @Override
  public Map<String, Object> createStateAttributes() throws PipelineStoreException {
    return runner.createStateAttributes();
  }

  @Override
  public void close() {
    runner.close();
  }

  @Override
  public Map<String, Object> updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    return runner.updateSlaveCallbackInfo(callbackInfo);
  }

  @Override
  public String getToken() {
    return runner.getToken();
  }

  @Override
  public int getRunnerCount() {
    return runner.getRunnerCount();
  }

  @Override
  public Runner getDelegatingRunner() {
    return runner;
  }
}
