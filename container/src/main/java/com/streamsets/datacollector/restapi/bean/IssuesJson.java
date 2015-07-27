/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
