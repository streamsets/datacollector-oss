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
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.util.PipelineException;

import java.util.List;
import java.util.Map;

public interface PipelineStoreTask extends Task {
  // Provide upgrade path in PipelineConfigurationUpgrader when increasing
  public static final int SCHEMA_VERSION = 5;
  public static final int RULE_DEFINITIONS_SCHEMA_VERSION = 3;

  public PipelineConfiguration create(
      String user,
      String pipelineId,
      String pipelineTitle,
      String description,
      boolean isRemote,
      boolean draft
  ) throws PipelineException;

  public void delete(String name) throws PipelineException;

  public List<PipelineInfo> getPipelines() throws PipelineStoreException;

  public PipelineInfo getInfo(String name) throws PipelineException;

  public List<PipelineRevInfo> getHistory(String name) throws PipelineException;

  public PipelineConfiguration save(String user, String name, String tag, String tagDescription,
      PipelineConfiguration pipeline) throws PipelineException;

  public PipelineConfiguration load(String name, String tagOrRev) throws PipelineException;

  public boolean hasPipeline(String name) throws PipelineException;

  public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineException;

  public RuleDefinitions storeRules(String pipelineName, String tag, RuleDefinitions ruleDefinitions, boolean draft)
      throws PipelineException;

  public boolean deleteRules(String name) throws PipelineException;

  public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException;

  public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineException;

  public PipelineConfiguration saveMetadata(
      String user,
      String name,
      String rev,
      Map<String, Object> metadata
  ) throws PipelineException;

  void registerStateListener(StateEventListener stateListener);

}
