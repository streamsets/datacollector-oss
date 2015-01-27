/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.util.NullDeserializer;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class RuleIssue extends Issue {

  private final String ruleId;

  public static RuleIssue createRuleIssue(String ruleId, ValidationError error, Object... args) {
    return new RuleIssue(ruleId, error, args);
  }

  private RuleIssue(String ruleId, ValidationError error, Object... args) {
    super(error, args);
    this.ruleId = ruleId;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String toString() {
    return Utils.format("Rule '{}': {}", getRuleId(), super.toString());
  }
}
