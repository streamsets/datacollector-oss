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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;

public class IssuesJson {

  private final com.streamsets.datacollector.validation.Issues issues;

  public IssuesJson(com.streamsets.datacollector.validation.Issues issues) {
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
  public com.streamsets.datacollector.validation.Issues getIssues() {
    return issues;
  }
}
