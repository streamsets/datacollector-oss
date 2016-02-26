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
package com.streamsets.datacollector.config.dto;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Issues implements Serializable {
  private List<Issue> issues;
  private List<Issue> pipelineIssues;
  private Map<String, List<Issue>> stageIssues;

  public Issues() {
    issues = new ArrayList<>();
    pipelineIssues = new ArrayList<>();
    stageIssues = new HashMap<>();
  }

  public Issues(List<Issue> issues) {
    this();
    addAll(issues);
  }

  public void setIssues(List<Issue> issues) {
    this.issues = issues;
  }

  public void setPipelineIssues(List<Issue> pipelineIssues) {
    this.pipelineIssues = pipelineIssues;
  }

  public List<Issue> getIssues() {
    return issues;
  }

  public List<Issue> getPipelineIssues() {
    return pipelineIssues;
  }

  public Map<String, List<Issue>> getStageIssues() {
    return stageIssues;
  }

  public void setStageIssues(Map<String, List<Issue>> stageIssues) {
    this.stageIssues = stageIssues;
  }

  public void addAll(List<Issue> issues) {
    for (Issue issue : issues) {
      add(issue);
    }
  }

  public void add(Issue issue) {
    issues.add(issue);
    String instance = issue.getInstanceName();
    if (instance == null) {
      pipelineIssues.add(issue);
    } else {
      List<Issue> stage = stageIssues.get(instance);
      if (stage == null) {
        stage = new ArrayList<>();
        stageIssues.put(instance, stage);
      }
      stage.add(issue);
    }
  }

  public boolean hasIssues() {
    return !issues.isEmpty();
  }

  // Selma has issues with issueCount() method
  public int retrieveIssueSize() {
    return issues.size();
  }

  @Override
  public String toString() {
    return Utils.format("Issues[pipeline='{}' stage='{}']", pipelineIssues.size(), stageIssues.size());
  }

}
