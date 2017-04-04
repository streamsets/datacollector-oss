/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.task.Task;
import com.streamsets.pipeline.api.impl.annotationsprocessor.PipelineAnnotationsProcessor;

import java.util.List;
import java.util.Map;

public interface StageLibraryTask extends Task, ClassLoaderReleaser {

  public static final String STAGES_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.STAGES_FILE;

  public static final String EL_DEFINITION_RESOURCE = PipelineAnnotationsProcessor.ELDEFS_FILE;

  public PipelineDefinition getPipeline();

  public PipelineRulesDefinition getPipelineRules();

  public List<StageDefinition> getStages();

  public StageDefinition getStage(String library, String name, boolean forExecution);

  public Map<String, String> getLibraryNameAliases();

  public Map<String, String> getStageNameAliases();

}
