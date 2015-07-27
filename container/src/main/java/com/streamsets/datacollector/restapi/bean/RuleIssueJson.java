/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.datacollector.util.NullDeserializer;

import java.util.Map;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class RuleIssueJson {

  private final com.streamsets.datacollector.validation.RuleIssue ruleIssue;

  public RuleIssueJson(com.streamsets.datacollector.validation.RuleIssue ruleIssue) {
    this.ruleIssue = ruleIssue;
  }

  public Map getAdditionalInfo() {
    return ruleIssue.getAdditionalInfo();
  }

  public String getMessage() {
    return ruleIssue.getMessage();
  }

  public String getRuleId() {
    return ruleIssue.getRuleId();
  }

  @JsonIgnore
  public com.streamsets.datacollector.validation.RuleIssue getRuleIssue() {
    return ruleIssue;
  }
}
