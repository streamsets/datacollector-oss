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
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PreviewStageLibraryTask extends TaskWrapper implements StageLibraryTask {
  public static final String LIBRARY = ":system:";
  public static final String NAME = ":plug:";
  public static final int VERSION = 1;

  private static final StageLibraryDefinition PREVIEW_LIB = new StageLibraryDefinition(
      PreviewStageLibraryTask.class.getClassLoader(), LIBRARY, "Preview", new Properties(), null, null, null);

  private static final StageDefinition PLUG_STAGE =  new StageDefinition(
      PREVIEW_LIB,
      false,
      PreviewPlugTarget.class,
      NAME,
      VERSION,
      "previewPlug",
      "Preview Plug",
      StageType.TARGET,
      false,
      false,
      false,
      Collections.<ConfigDefinition>emptyList(),
      null,
      "",
      null,
      false,
      0,
      null,
      Arrays.asList(ExecutionMode.STANDALONE),
      false,
      new StageUpgrader.Default(),
      Collections.<String>emptyList(),
      false,
      "",
      false,
      false,
      false,
      false,
      Collections.emptyList()
  );

  private final StageLibraryTask library;

  public PreviewStageLibraryTask(StageLibraryTask library) {
    super(library);
    this.library = library;
  }

  @Override
  public PipelineDefinition getPipeline() {
    return library.getPipeline();
  }

  @Override
  public PipelineRulesDefinition getPipelineRules() {
    return library.getPipelineRules();
  }

  @Override
  public List<StageDefinition> getStages() {
    return library.getStages();
  }

  @Override
  public List<LineagePublisherDefinition> getLineagePublisherDefinitions() {
    return library.getLineagePublisherDefinitions();
  }

  @Override
  public LineagePublisherDefinition getLineagePublisherDefinition(String library, String name) {
    return this.library.getLineagePublisherDefinition(library, name);
  }

  @Override
  public List<CredentialStoreDefinition> getCredentialStoreDefinitions() {
    return library.getCredentialStoreDefinitions();
  }

  @Override
  public List<ServiceDefinition> getServiceDefinitions() {
    return library.getServiceDefinitions();
  }

  @Override
  public ServiceDefinition getServiceDefinition(Class serviceInterface, boolean forExecution) {
    return library.getServiceDefinition(serviceInterface, forExecution);
  }

  @Override
  public StageDefinition getStage(String library, String name, boolean forExecution) {
    StageDefinition def;
    if (LIBRARY.equals(library) && NAME.equals(name)) {
      def = PLUG_STAGE;
    } else {
      def = this.library.getStage(library, name, forExecution);
    }
    return def;
  }

  @Override
  public Map<String, String> getLibraryNameAliases() {
    return library.getLibraryNameAliases();
  }

  @Override
  public Map<String, String> getStageNameAliases() {
    return library.getStageNameAliases();
  }

  @Override
  public List<ClasspathValidatorResult> validateStageLibClasspath() {
    return library.validateStageLibClasspath();
  }

  @Override
  public void releaseStageClassLoader(ClassLoader classLoader) {
    if (classLoader != PLUG_STAGE.getClass().getClassLoader()) {
      library.releaseStageClassLoader(classLoader);
    }
  }

}
