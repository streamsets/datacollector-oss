/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class StageIssueJson {

  private final com.streamsets.pipeline.validation.StageIssue stageIssue;

  public StageIssueJson(com.streamsets.pipeline.validation.StageIssue stageIssue) {
    this.stageIssue = stageIssue;
  }

  public String getInstanceName() {
    return stageIssue.getInstanceName();
  }

  public boolean isErrorStage() {
    return stageIssue.isErrorStage();
  }

  public String getLevel() {
    return stageIssue.getLevel();
  }

  public Map getAdditionalInfo() {
    return stageIssue.getAdditionalInfo();
  }

  public String getMessage() { return stageIssue.getMessage();
  }

  public String getConfigGroup() {
    return stageIssue.getConfigGroup();
  }

  public String getConfigName() {
    return stageIssue.getConfigName();
  }

  @JsonIgnore
  public com.streamsets.pipeline.validation.StageIssue getStageIssue() {
    return stageIssue;
  }
}
