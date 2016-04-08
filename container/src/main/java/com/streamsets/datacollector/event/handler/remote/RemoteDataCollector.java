/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.event.handler.remote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

public class RemoteDataCollector implements DataCollector {

  public static final String IS_REMOTE_PIPELINE = "IS_REMOTE_PIPELINE";
  private static final String NAME_AND_REV_SEPARATOR = "::";
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDataCollector.class);
  private final Manager manager;
  private final PipelineStoreTask pipelineStore;
  private final List<String> validatorIdList;
  private final PipelineStateStore pipelineStateStore;

  @Inject
  public RemoteDataCollector(Manager manager, PipelineStoreTask pipelineStore, PipelineStateStore pipelineStateStore) {
    this.manager = manager;
    this.pipelineStore = pipelineStore;
    this.pipelineStateStore = pipelineStateStore;
    this.validatorIdList = new ArrayList<String>();
  }

  private void validateIfRemote(String name, String rev, String operation) throws PipelineException {
    if (!manager.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01100, operation, name);
    }
  }

  @Override
  public void start(String user, String name, String rev) throws PipelineException, StageException {
    validateIfRemote(name, rev, "START");
    manager.getRunner(user, name, rev).start();
  }

  @Override
  public void stop(String user, String name, String rev) throws PipelineException {
    validateIfRemote(name, rev, "STOP");
    manager.getRunner(user, name, rev).stop();
  }

  @Override
  public void delete(String name, String rev) throws PipelineException {
    validateIfRemote(name, rev, "DELETE");
    pipelineStore.delete(name);
    pipelineStore.deleteRules(name);
  }

  @Override
  public void deleteHistory(String user, String name, String rev) throws PipelineException {
    validateIfRemote(name, rev, "DELETE_HISTORY");
    manager.getRunner(user, name, rev).deleteHistory();
  }

  @Override
  public void savePipeline(String user,
    String name,
    String rev,
    String description,
    PipelineConfiguration pipelineConfiguration,
    RuleDefinitions ruleDefinitions) throws PipelineException {

    List<PipelineState> pipelineInfoList = manager.getPipelines();
    boolean pipelineExists = false;
    for (PipelineState pipelineState : pipelineInfoList) {
      if (pipelineState.getName().equals(name)) {
        pipelineExists = true;
        break;
      }
    }
    UUID uuid;
    if (!pipelineExists) {
      uuid = pipelineStore.create(user, name, description, true).getUuid();
    } else {
      validateIfRemote(name, rev, "SAVE");
      PipelineInfo pipelineInfo = pipelineStore.getInfo(name);
      uuid = pipelineInfo.getUuid();
      ruleDefinitions.setUuid(pipelineStore.retrieveRules(name, rev).getUuid());
    }
    pipelineConfiguration.setUuid(uuid);
    pipelineStore.save(user, name, rev, description, pipelineConfiguration);
    pipelineStore.storeRules(name, rev, ruleDefinitions);
  }

  @Override
  public void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineException {
    validateIfRemote(name, rev, "SAVE_RULES");
    // Check for existence of pipeline first
    pipelineStore.getInfo(name);
    ruleDefinitions.setUuid(pipelineStore.retrieveRules(name, rev).getUuid());
    pipelineStore.storeRules(name, rev, ruleDefinitions);
  }

  @Override
  public void resetOffset(String user, String name, String rev) throws PipelineException,
    PipelineManagerException {
    validateIfRemote(name, rev, "RESET_OFFSET");
    manager.getRunner(user, name, rev).resetOffset();
  }

  @Override
  public void validateConfigs(String user, String name, String rev) throws PipelineException {
    Previewer previewer = manager.createPreviewer(user, name, rev);
    validateIfRemote(name, rev, "VALIDATE_CONFIGS");
    previewer.validateConfigs(1000L);
    validatorIdList.add(previewer.getId());
  }

  @Override
  public void stopAndDelete(String user, String name, String rev) throws PipelineException, StageException {
    validateIfRemote(name, rev, "STOP_AND_DELETE");
    manager.getRunner(user, name, rev).stop();
    PipelineState pipelineState = pipelineStateStore.getState(name, rev);
    long now = System.currentTimeMillis();
    // wait for 10 secs for a graceful stop
    while (pipelineState.getStatus().isActive() && (System.currentTimeMillis() - now) < 10000) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting for pipeline to stop " + e, e);
      }
      pipelineState = pipelineStateStore.getState(name, rev);
    }
    // If still active, force stop of this pipeline as we are deleting this anyways
    if (pipelineState.getStatus().isActive()) {
      pipelineStateStore.saveState(user, name, rev, PipelineStatus.STOPPED,
        "Stopping pipeline forcefully as we are performing a delete afterwards", pipelineState.getAttributes(), pipelineState.getExecutionMode(),
        pipelineState.getMetrics(), pipelineState.getRetryAttempt(), pipelineState.getNextRetryTimeStamp());
    }
    delete(name, rev);
  }

  @Override
  public Collection<PipelineAndValidationStatus> getPipelines() throws PipelineException {
    List<PipelineState> pipelineStates = manager.getPipelines();
    Map<String, PipelineAndValidationStatus> pipelineStatusMap = new HashMap<String, PipelineAndValidationStatus>();
    for (PipelineState pipelineState : pipelineStates) {
      boolean isRemote = false;
      String name = pipelineState.getName();
      String rev = pipelineState.getRev();
      if (manager.isRemotePipeline(name, rev)) {
        isRemote = true;
      }
      // ignore local and non active pipelines
      if (isRemote || manager.isPipelineActive(name, rev)) {
        pipelineStatusMap.put(getNameAndRevString(name, rev),
          new PipelineAndValidationStatus(name, rev, isRemote, pipelineState.getStatus(), pipelineState.getMessage()));
      }
    }
    setValidationStatus(pipelineStatusMap);
    return pipelineStatusMap.values();
  }

  private void setValidationStatus(Map<String, PipelineAndValidationStatus> pipelineStatusMap) {
    List<String> idsToRemove = new ArrayList<String>();
    for (String previewerId : validatorIdList) {
      Previewer previewer = manager.getPreviewer(previewerId);
      ValidationStatus validationStatus = null;
      Issues issues = null;
      String message = null;
      if (previewer != null) {
        PreviewStatus previewStatus = previewer.getStatus();
        switch (previewStatus) {
          case INVALID:
            validationStatus = ValidationStatus.INVALID;
            break;
          case TIMING_OUT:
          case TIMED_OUT:
            validationStatus = ValidationStatus.TIMED_OUT;
            break;
          case VALID:
            validationStatus = ValidationStatus.VALID;
            break;
          case VALIDATING:
            validationStatus = ValidationStatus.VALIDATING;
            break;
          case VALIDATION_ERROR:
            validationStatus = ValidationStatus.VALIDATION_ERROR;
            break;
          default:
            LOG.warn(Utils.format("Unrecognized validation state: '{}'", previewStatus));
        }
        if (!previewStatus.isActive()) {
          PreviewOutput previewOutput = previewer.getOutput();
          issues = previewOutput.getIssues();
          message = previewOutput.getMessage();
          idsToRemove.add(previewerId);
        }
      } else {
        LOG.warn(Utils.format("Previewer is null for id: '{}'", previewerId));
      }
      PipelineAndValidationStatus pipelineAndValidationStatus =
        pipelineStatusMap.get(getNameAndRevString(previewer.getName(), previewer.getRev()));
      if (pipelineAndValidationStatus == null) {
        LOG.warn("Preview pipeline: '{}'::'{}' doesn't exist", previewer.getName(), previewer.getRev());
      } else {
        pipelineAndValidationStatus.setValidationStatus(validationStatus);
        pipelineAndValidationStatus.setIssues(issues);
        pipelineAndValidationStatus.setMessage(message);
      }
    }
    for (String id : idsToRemove) {
      validatorIdList.remove(id);
    }
  }

  private String getNameAndRevString(String name, String rev) {
    return name + NAME_AND_REV_SEPARATOR + rev;
  }

  @VisibleForTesting
  List<String> getValidatorList() {
    return validatorIdList;
  }

}

