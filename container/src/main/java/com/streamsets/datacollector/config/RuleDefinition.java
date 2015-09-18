/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
