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
package com.streamsets.datacollector.config;

public abstract class RuleDefinition {
  private final String family;
  private final String id;
  private final String alertText;
  private final String condition;
  private final boolean sendEmail;
  private final boolean enabled;
  private boolean valid = true;
  private final long timestamp;

  public RuleDefinition(
      String family,
      String id,
      String condition,
      String alertText,
      boolean sendEmail,
      boolean enabled,
      long timestamp
  ) {
    this.family = family;
    this.id = id;
    this.alertText = alertText;
    this.condition = condition;
    this.sendEmail = sendEmail;
    this.enabled = enabled;
    this.timestamp = timestamp;
  }

  public String getFamily() {
    return family;
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

  public long getTimestamp() {
    return timestamp;
  }
}
