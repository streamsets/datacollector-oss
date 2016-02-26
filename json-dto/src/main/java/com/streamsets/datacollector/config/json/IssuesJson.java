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
package com.streamsets.datacollector.config.json;

import java.util.List;
import java.util.Map;

public class IssuesJson {
  private List<IssueJson> issues;
  private List<IssueJson> pipelineIssues;
  private Map<String, List<IssueJson>> stageIssues;

  public List<IssueJson> getIssues() {
    return issues;
  }

  public void setIssues(List<IssueJson> issues) {
    this.issues = issues;
  }

  public List<IssueJson> getPipelineIssues() {
    return pipelineIssues;
  }

  public void setPipelineIssues(List<IssueJson> pipelineIssues) {
    this.pipelineIssues = pipelineIssues;
  }

  public Map<String, List<IssueJson>> getStageIssues() {
    return stageIssues;
  }

  public void setStageIssues(Map<String, List<IssueJson>> stageIssues) {
    this.stageIssues = stageIssues;
  }
}
