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
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRunner {

  public static class Runner1 implements Runner {
    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getRev() {
      return null;
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return null;
    }

    @Override
    public String getPipelineTitle() throws PipelineException {
      return null;
    }

    @Override
    public void resetOffset(String user) throws PipelineException {

    }

    @Override
    public SourceOffset getCommittedOffsets() throws PipelineException {
      return null;
    }

    @Override
    public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {

    }

    @Override
    public PipelineState getState() throws PipelineStoreException {
      return null;
    }

    @Override
    public void prepareForDataCollectorStart(String user) throws PipelineException {

    }

    @Override
    public void onDataCollectorStart(String user) throws PipelineException, StageException {

    }

    @Override
    public void onDataCollectorStop(String user) throws PipelineException {

    }

    @Override
    public void stop(String user) throws PipelineException {

    }

    @Override
    public void forceQuit(String user) throws PipelineException {

    }

    @Override
    public void prepareForStart(StartPipelineContext context) throws PipelineException {

    }

    @Override
    public void prepareForStop(String user) throws PipelineException {

    }

    @Override
    public void start(StartPipelineContext context) throws PipelineException, StageException {

    }

    @Override
    public void startAndCaptureSnapshot(StartPipelineContext context, String snapshotName, String snapshotLabel, int batches, int batchSize) throws PipelineException, StageException {

    }

    @Override
    public String captureSnapshot(String user, String snapshotName, String snapshotLabel, int batches, int batchSize)
        throws PipelineException {
      return null;
    }

    @Override
    public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
      return null;
    }

    @Override
    public Snapshot getSnapshot(String id) throws PipelineException {
      return null;
    }

    @Override
    public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
      return null;
    }

    @Override
    public void deleteSnapshot(String id) throws PipelineException {

    }

    @Override
    public List<PipelineState> getHistory() throws PipelineStoreException {
      return null;
    }

    @Override
    public void deleteHistory() throws PipelineException {

    }

    @Override
    public Object getMetrics() throws PipelineException {
      return null;
    }

    @Override
    public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<ErrorMessage> getErrorMessages(String stage, int max)
        throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<SampledRecord> getSampledRecords(
        String sampleId, int max
    ) throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<AlertInfo> getAlerts() throws PipelineException {
      return null;
    }

    @Override
    public boolean deleteAlert(String alertId) throws PipelineException {
      return false;
    }

    @Override
    public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
      return null;
    }

    @Override
    public Map<String, Object> createStateAttributes() {
      return new HashMap<>();
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
    public Runner getDelegatingRunner() {
      return null;
    }

    @Override
    public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
      return null;
    }
  }

  public class Runner2 implements Runner {
    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getRev() {
      return null;
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return null;
    }

    @Override
    public String getPipelineTitle() throws PipelineException {
      return null;
    }

    @Override
    public void resetOffset(String user) throws PipelineException {

    }

    @Override
    public SourceOffset getCommittedOffsets() throws PipelineException {
      return null;
    }

    @Override
    public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {

    }

    @Override
    public PipelineState getState() throws PipelineStoreException {
      return null;
    }

    @Override
    public void prepareForDataCollectorStart(String user) throws PipelineException {

    }

    @Override
    public void onDataCollectorStart(String user) throws PipelineException, StageException {

    }

    @Override
    public void onDataCollectorStop(String user) throws PipelineException {

    }

    @Override
    public void stop(String user) throws PipelineException {

    }

    @Override
    public void forceQuit(String user) throws PipelineException {

    }

    @Override
    public void prepareForStart(StartPipelineContext context) throws PipelineException {

    }

    @Override
    public void prepareForStop(String user) throws PipelineException {

    }

    @Override
    public void start(StartPipelineContext context) throws PipelineException, StageException {

    }

    @Override
    public void startAndCaptureSnapshot(StartPipelineContext context, String snapshotName, String snapshotLabel, int batches, int batchSize) throws PipelineException, StageException {

    }

    @Override
    public String captureSnapshot(String user, String snapshotName, String snapshotLabel, int batches, int batchSize)
        throws PipelineException {
      return null;
    }

    @Override
    public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
      return null;
    }

    @Override
    public Snapshot getSnapshot(String id) throws PipelineException {
      return null;
    }

    @Override
    public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
      return null;
    }

    @Override
    public void deleteSnapshot(String id) throws PipelineException {

    }

    @Override
    public List<PipelineState> getHistory() throws PipelineStoreException {
      return null;
    }

    @Override
    public void deleteHistory() throws PipelineException {

    }

    @Override
    public Object getMetrics() throws PipelineException {
      return null;
    }

    @Override
    public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<ErrorMessage> getErrorMessages(String stage, int max)
        throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<SampledRecord> getSampledRecords(String sampleId, int max)
        throws PipelineRunnerException, PipelineStoreException {
      return null;
    }

    @Override
    public List<AlertInfo> getAlerts() throws PipelineException {
      return null;
    }

    @Override
    public boolean deleteAlert(String alertId) throws PipelineException {
      return false;
    }

    @Override
    public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
      return null;
    }

    @Override
    public Map<String, Object> createStateAttributes() {
      return new HashMap<>();
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
    public Runner getDelegatingRunner() {
      return new Runner1();
    }

    @Override
    public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
      return null;
    }
  }

  @Test
  public void testGetDelegatingRunner() {
    Runner1 r1 = new Runner1();
    Assert.assertEquals(r1, r1.getRunner(Runner1.class));
    Assert.assertNull(r1.getRunner(Runner2.class));

    Runner2 r2 = new Runner2();
    Assert.assertNotNull(r2.getRunner(Runner1.class));
    Assert.assertEquals(Runner1.class, r2.getRunner(Runner1.class).getClass());

  }

}
