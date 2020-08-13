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
package com.streamsets.datacollector.event.handler;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.handler.remote.PipelineAndValidationStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.StageException;

public interface DataCollector {

  /**
   * initializes the DataCollector
   */
  void init();

  void start(Runner.StartPipelineContext context, String name, String rev, Set<String> groups) throws PipelineException, StageException;

  void stop(String user, String name, String rev) throws PipelineException;

  void delete(String name, String rev) throws PipelineException;

  void deleteHistory(String user, String name, String rev) throws PipelineException;

  String savePipeline(
      String user,
      String name,
      String rev,
      String description,
      SourceOffset offset,
      PipelineConfiguration pipelineConfiguration,
      RuleDefinitions ruleDefinitions,
      Acl acl,
      Map<String, Object> metadata,
      Map<String, ConnectionConfiguration> connections
  ) throws PipelineException;

  void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineException;

  void resetOffset(String user, String name, String rev) throws PipelineException;

  void validateConfigs(
      String user,
      String name,
      String rev,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs
  ) throws PipelineException;

  /**
   * Preview a pipeline.
   *
   * @param user
   * @param name
   * @param rev
   * @param batches
   * @param batchSize
   * @param skipTargets
   * @param skipLifecycleEvents
   * @param stopStage
   * @param stagesOverride
   * @param timeoutMillis
   * @param testOrigin
   * @param interceptorConfs the list of interceptor configs to use
   * @return the previewer ID
   * @throws PipelineException
   */
  String previewPipeline(
      String user,
      String name,
      String rev,
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      List<StageOutput> stagesOverride,
      long timeoutMillis,
      boolean testOrigin,
      List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
      Function<Object, Void> afterActionsFunction,
      Map<String, ConnectionConfiguration> connections
  ) throws PipelineException;

  Future<AckEvent> stopAndDelete(String user, String name, String rev,
                                 long forceStopMillis) throws PipelineException, StageException;

  Collection<PipelineAndValidationStatus> getPipelines() throws PipelineException, IOException;

  List<PipelineAndValidationStatus> getRemotePipelinesWithChanges() throws PipelineException;

  void syncAcl(Acl acl) throws PipelineException;

  /**
   * Add a new object to DataCollector's blob store.
   */
  void blobStore(String namespace, String id, long version, String content) throws StageException;

  /**
   * Remove all versions of given object from DataCollector's blob store.
   */
  void blobDelete(String namespace, String id) throws StageException;

  /**
   * Remove object from DataCollector's blob store.
   */
  void blobDelete(String namespace, String id, long version) throws StageException;

  /**
   * Store new configuration from control hub inside this data collector in a persistent manner.
   */
  void storeConfiguration(Map<String, String> newConfiguration) throws IOException;

  /**
   * Get runner for a pipeline
   */
  Runner getRunner(String runner, String rev) throws PipelineException;

  List<PipelineState> getRemotePipelines() throws PipelineException;
}
