/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class IssueJson {

  private final com.streamsets.pipeline.validation.Issue issue;

  public IssueJson(com.streamsets.pipeline.validation.Issue issue) {
    this.issue = issue;
  }

  public Map getAdditionalInfo() {
    return issue.getAdditionalInfo();
  }

  public String getMessage() { return issue.getMessage();
  }

  public String getConfigGroup() {
    return issue.getConfigGroup();
  }

  public String getConfigName() {
    return issue.getConfigName();
  }

  @JsonIgnore
  public com.streamsets.pipeline.validation.Issue getIssue() {
    return issue;
  }
}