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
import java.util.concurrent.Future;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.handler.remote.PipelineAndValidationStatus;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.StageException;

public interface DataCollector {

  void start(String user, String name, String rev) throws PipelineException, StageException;

  void stop(String user, String name, String rev) throws PipelineException;

  void delete(String name, String rev) throws PipelineException;

  void deleteHistory(String user, String name, String rev) throws PipelineException;

  void savePipeline(
      String user,
      String name,
      String rev,
      String description,
      SourceOffset offset,
      PipelineConfiguration pipelineConfiguration,
      RuleDefinitions ruleDefinitions,
      Acl acl
  ) throws PipelineException;

  void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineException;

  void resetOffset(String user, String name, String rev) throws PipelineException;

  void validateConfigs(String user, String name, String rev) throws PipelineException;

  Future<AckEvent> stopAndDelete(String user, String name, String rev,
                                 long forceStopMillis) throws PipelineException, StageException;

  Collection<PipelineAndValidationStatus> getPipelines() throws PipelineException, IOException;

  List<PipelineAndValidationStatus> getRemotePipelinesWithChanges() throws PipelineException;

  void syncAcl(Acl acl) throws PipelineException;

}
