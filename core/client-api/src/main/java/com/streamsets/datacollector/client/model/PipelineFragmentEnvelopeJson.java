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
package com.streamsets.datacollector.client.model;

public class PipelineFragmentEnvelopeJson {
  private PipelineFragmentConfigurationJson pipelineFragmentConfig;
  private RuleDefinitionsJson pipelineRules;
  private DefinitionsJson libraryDefinitions;

  public PipelineFragmentConfigurationJson getPipelineFragmentConfig() {
    return pipelineFragmentConfig;
  }

  public void setPipelineFragmentConfig(PipelineFragmentConfigurationJson pipelineFragmentConfig) {
    this.pipelineFragmentConfig = pipelineFragmentConfig;
  }

  public RuleDefinitionsJson getPipelineRules() {
    return pipelineRules;
  }

  public void setPipelineRules(RuleDefinitionsJson pipelineRules) {
    this.pipelineRules = pipelineRules;
  }

  public DefinitionsJson getLibraryDefinitions() {
    return libraryDefinitions;
  }

  public void setLibraryDefinitions(DefinitionsJson libraryDefinitions) {
    this.libraryDefinitions = libraryDefinitions;
  }
}
