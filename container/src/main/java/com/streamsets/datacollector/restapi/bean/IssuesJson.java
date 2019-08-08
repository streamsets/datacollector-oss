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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.validation.Issues;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IssuesJson {

  private final Issues issues;

  @JsonCreator
  public IssuesJson(
      @JsonProperty("pipelineIssues") List<IssueJson> pipelineIssues,
      @JsonProperty("stageIssues") Map<String, List<IssueJson>> stageIssues,
      @JsonProperty("issueCount") int issueCount
  ) {
    this.issues = new Issues();
    if (pipelineIssues != null) {
      issues.addAll(BeanHelper.unwrapIssues(pipelineIssues));
    }
    for(Map.Entry<String, List<IssueJson>> e : stageIssues.entrySet()) {
      if (e.getValue() != null) {
        issues.addAll(BeanHelper.unwrapIssues(e.getValue()));
      }
    }
  }

  public IssuesJson(Issues issues) {
    this.issues = issues;
  }

  public List<IssueJson> getPipelineIssues() {
    return BeanHelper.wrapIssues(issues.getPipelineIssues());
  }

  public Map<String, List<IssueJson>> getStageIssues() {
    return BeanHelper.wrapIssuesMap(issues.getStageIssues());
  }

  public int getIssueCount() {
    return issues.getIssueCount();
  }

  @JsonIgnore
  public Issues getIssues() {
    return issues;
  }
}
