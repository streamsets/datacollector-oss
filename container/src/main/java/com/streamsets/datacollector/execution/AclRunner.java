/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AclRunner implements Runner {
  private final Runner runner;
  private final AclStoreTask aclStore;
  private final UserJson currentUser;

  public AclRunner(Runner runner, AclStoreTask aclStore, UserJson currentUser) {
    this.runner = runner;
    this.aclStore = aclStore;
    this.currentUser = currentUser;
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
  public String getUser() {
    return runner.getUser();
  }

  @Override
  public void resetOffset() throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.resetOffset();
  }

  @Override
  public Map<String, String> getCommittedOffsets() throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    return runner.getCommittedOffsets();
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return runner.getState();
  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    runner.prepareForDataCollectorStart();
  }

  @Override
  public void onDataCollectorStart() throws PipelineException, StageException {
    runner.onDataCollectorStart();
  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    runner.onDataCollectorStop();
  }

  @Override
  public void stop() throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.stop();
  }

  @Override
  public void forceQuit() throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.forceQuit();
  }

  @Override
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException {
    runner.prepareForStart();
  }

  @Override
  public void prepareForStop() throws PipelineStoreException, PipelineRunnerException {
    runner.prepareForStop();
  }

  @Override
  public void start() throws PipelineException, StageException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.start();
  }

  @Override
  public void start(Map<String, Object> runtimeParameters) throws PipelineException, StageException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.start(runtimeParameters);
  }

  @Override
  public void startAndCaptureSnapshot(
      Map<String, Object> runtimeParameters,
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException, StageException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.startAndCaptureSnapshot(runtimeParameters, snapshotName, snapshotLabel, batches, batchSize);
  }

  @Override
  public String captureSnapshot(
      String snapshotName,
      String snapshotLabel,
      int batches,
      int batchSize
  ) throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
    return runner.captureSnapshot(snapshotName, snapshotLabel, batches, batchSize);
  }

  @Override
  public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
    aclStore.validateExecutePermission(this.getName(), currentUser);
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
    aclStore.validateExecutePermission(this.getName(), currentUser);
    runner.deleteSnapshot(id);
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return runner.getHistory();
  }

  @Override
  public void deleteHistory() throws PipelineException {
    aclStore.validateWritePermission(this.getName(), currentUser);
    runner.deleteHistory();
  }

  @Override
  public Object getMetrics() throws PipelineStoreException {
    return runner.getMetrics();
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
    return runner.getErrorRecords(stage, max);
  }

  @Override
  public List<ErrorMessage> getErrorMessages(
      String stage,
      int max
  ) throws PipelineRunnerException, PipelineStoreException {
    return runner.getErrorMessages(stage, max);
  }

  @Override
  public List<SampledRecord> getSampledRecords(
      String sampleId,
      int max
  ) throws PipelineRunnerException, PipelineStoreException {
    return runner.getSampledRecords(sampleId, max);
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineException {
    return runner.getAlerts();
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineException {
    aclStore.validateWritePermission(this.getName(), currentUser);
    return runner.deleteAlert(alertId);
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
    return runner.getSlaveCallbackList(callbackObjectType);
  }

  @Override
  public void close() {
    runner.close();
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    runner.updateSlaveCallbackInfo(callbackInfo);
  }

  @Override
  public Map getUpdateInfo() {
    return runner.getUpdateInfo();
  }

  @Override
  public String getToken() {
    return runner.getToken();
  }
}
