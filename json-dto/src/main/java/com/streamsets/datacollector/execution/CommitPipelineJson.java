/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.execution;

public class CommitPipelineJson {
  private String name;
  private String commitMessage;
  private String pipelineDefinition;
  private String libraryDefinitions;
  private String rulesDefinition;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCommitMessage() {
    return commitMessage;
  }

  public void setCommitMessage(String commitMessage) {
    this.commitMessage = commitMessage;
  }

  public String getPipelineDefinition() {
    return pipelineDefinition;
  }

  public void setPipelineDefinition(String pipelineDefinition) {
    this.pipelineDefinition = pipelineDefinition;
  }

  public String getLibraryDefinitions() {
    return libraryDefinitions;
  }

  public void setLibraryDefinitions(String libraryDefinitions) {
    this.libraryDefinitions = libraryDefinitions;
  }

  public String getRulesDefinition() {
    return rulesDefinition;
  }

  public void setRulesDefinition(String rulesDefinition) {
    this.rulesDefinition = rulesDefinition;
  }

}
