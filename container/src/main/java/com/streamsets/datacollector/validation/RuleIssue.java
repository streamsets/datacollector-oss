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
package com.streamsets.datacollector.validation;

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
