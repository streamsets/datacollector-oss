/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class RuleIssue {
  private final String ruleId;
  private final LocalizableString message;
  private Map<String, Object> additionalInfo;

  public static RuleIssue createRuleIssue(String ruleId, ValidationError error, Object... args) {
    return new RuleIssue(ruleId, error, args);
  }

  private RuleIssue(String ruleId, ValidationError error, Object... args) {
    this.ruleId = ruleId;
    message = new ErrorMessage(error, args);
  }

  public void setAdditionalInfo(String key, Object value) {
    if (additionalInfo == null) {
      additionalInfo = new HashMap<>();
    }
    additionalInfo.put(key, value);
  }

  public Map getAdditionalInfo() {
    return additionalInfo;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getMessage() {
    return message.getLocalized();
  }

  public String toString() {
    return Utils.format("Rule[id='{}' message='{}']", ruleId, message);
  }

}
