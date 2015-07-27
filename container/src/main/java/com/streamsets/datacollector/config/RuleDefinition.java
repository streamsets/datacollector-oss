/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RuleDefinition {

  private final String id;
  private final String alertText;
  private final String condition;
  private final boolean sendEmail;
  private final boolean enabled;
  private boolean valid = true;

  @JsonCreator
  public RuleDefinition(@JsonProperty("id") String id,
                        @JsonProperty("condition") String condition,
                        @JsonProperty("alertText") String alertText,
                        @JsonProperty("sendEmail") boolean sendEmail,
                        @JsonProperty("enabled") boolean enabled) {
    this.id = id;
    this.alertText = alertText;
    this.condition = condition;
    this.sendEmail = sendEmail;
    this.enabled = enabled;
  }

  public String getId() {
    return id;
  }

  public String getAlertText() {
    return alertText;
  }

  public String getCondition() {
    return condition;
  }

  public boolean isSendEmail() {
    return sendEmail;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isValid() {
    return valid;
  }

  public void setValid(boolean valid) {
    this.valid = valid;
  }
}
