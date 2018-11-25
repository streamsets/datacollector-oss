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

package com.streamsets.datacollector.event.handler.remote;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

//TODO - Remove this compatibility in next major release (SDC-10541)
public class ColonCompatibleRemoteDataCollector implements DataCollector {

  private RemoteDataCollector remoteDataCollector;

  public ColonCompatibleRemoteDataCollector(RemoteDataCollector dataCollector) {
    this.remoteDataCollector = dataCollector;
  }

  @VisibleForTesting
  String getCompatibleName(String pipelineName) {
    String name = pipelineName;
    try {
      if (!remoteDataCollector.getPipelineStoreTask().hasPipeline(pipelineName)) {
        String decodedName = decode(pipelineName);
        boolean b = remoteDataCollector.getPipelineStoreTask().hasPipeline(decodedName);
        if (b) {
          name = decodedName;
        }
      }
    } catch (PipelineException e) {
      throw new IllegalStateException(Utils.format("Unexpected exception {}", e), e);
    }
    return name;
  }


  private static String decode(String name) {
    return name.replaceAll("__", ":");
  }

  @Override
  public void init() {
    remoteDataCollector.init();
  }

  @Override
  public void start(
      Runner.StartPipelineContext context, String name, String rev
  ) throws PipelineException, StageException {
    remoteDataCollector.start(context, getCompatibleName(name), rev);
  }

  @Override
  public void stop(String user, String name, String rev) throws PipelineException {
    remoteDataCollector.stop(user, getCompatibleName(name), rev);
  }

  @Override
  public void delete(String name, String rev) throws PipelineException {
    remoteDataCollector.delete(getCompatibleName(name), rev);
  }

  @Override
  public void deleteHistory(String user, String name, String rev) throws PipelineException {
    remoteDataCollector.deleteHistory(user, getCompatibleName(name), rev);
  }

  @Override
  public void savePipeline(
      String user,
      String name,
      String rev,
      String description,
      SourceOffset offset,
      PipelineConfiguration pipelineConfiguration,
      RuleDefinitions ruleDefinitions,
      Acl acl,
      Map<String, Object> metadata
  ) throws PipelineException {
    remoteDataCollector.savePipeline(
        user,
        name,
        rev,
        description,
        offset,
        pipelineConfiguration,
        ruleDefinitions,
        acl,
        metadata
    );
  }

  @Override
  public void savePipelineRules(
      String name, String rev, RuleDefinitions ruleDefinitions
  ) throws PipelineException {
    remoteDataCollector.savePipelineRules(getCompatibleName(name), rev, ruleDefinitions);
  }

  @Override
  public void resetOffset(String user, String name, String rev) throws PipelineException {
    remoteDataCollector.resetOffset(user, getCompatibleName(name), rev);
  }

  @Override
  public void validateConfigs(String user, String name, String rev) throws PipelineException {
    remoteDataCollector.validateConfigs(user, getCompatibleName(name), rev);
  }

  @Override
  public Future<AckEvent> stopAndDelete(
      String user, String name, String rev, long forceStopMillis
  ) throws PipelineException, StageException {
    return remoteDataCollector.stopAndDelete(user, getCompatibleName(name), rev, forceStopMillis);
  }

  @Override
  public Collection<PipelineAndValidationStatus> getPipelines() throws PipelineException, IOException {
    return remoteDataCollector.getPipelines();
  }

  @Override
  public List<PipelineAndValidationStatus> getRemotePipelinesWithChanges() throws PipelineException {
    return remoteDataCollector.getRemotePipelinesWithChanges();
  }

  @Override
  public void syncAcl(Acl acl) throws PipelineException {
    remoteDataCollector.syncAcl(acl);
  }

  @Override
  public void blobStore(String namespace, String id, long version, String content) throws StageException {
    remoteDataCollector.blobStore(namespace, id, version, content);
  }

  @Override
  public void blobDelete(String namespace, String id) throws StageException {
    remoteDataCollector.blobDelete(namespace, id);
  }

  @Override
  public void blobDelete(String namespace, String id, long version) throws StageException {
    remoteDataCollector.blobDelete(namespace, id, version);
  }

  @Override
  public void storeConfiguration(Map<String, String> newConfiguration) throws IOException {
    remoteDataCollector.storeConfiguration(newConfiguration);
  }
}
