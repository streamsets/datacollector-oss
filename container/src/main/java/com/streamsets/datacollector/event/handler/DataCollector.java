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
package com.streamsets.datacollector.event.handler;

import java.util.Collection;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.event.handler.remote.PipelineAndValidationStatus;
import com.streamsets.datacollector.util.PipelineException;
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
    PipelineConfiguration pipelineConfiguration,
    RuleDefinitions ruleDefinitions) throws PipelineException;

  void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineException;

  void resetOffset(String user, String name, String rev) throws PipelineException;

  void validateConfigs(String user, String name, String rev) throws PipelineException;

  void stopAndDelete(String user, String name, String rev) throws PipelineException, StageException;

  Collection<PipelineAndValidationStatus> getPipelines() throws PipelineException;

}
