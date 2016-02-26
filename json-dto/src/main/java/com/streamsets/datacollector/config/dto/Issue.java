/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.config.dto;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Issue implements Serializable {
  private String instanceName;
  private String configGroup;
  private String configName;
  private LocalizableString message;
  private String localizedMessage;
  private Map<String, Object> additionalInfo;

  public Issue() {
  }

  protected Issue(String instanceName, String configGroup, String configName, ErrorCode error, Object... args) {
    this.instanceName = instanceName;
    this.configGroup = configGroup;
    this.configName = configName;
    message = new ErrorMessage(error, args);
  }

  public String getLocalizedMessage() {
    return localizedMessage;
  }

  public void setLocalizedMessage(String localizedMessage) {
    this.localizedMessage = localizedMessage;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public void setConfigGroup(String configGroup) {
    this.configGroup = configGroup;
  }

  public void setConfigName(String configName) {
    this.configName = configName;
  }

  public void setAdditionalInfo(Map<String, Object> additionalInfo) {
    this.additionalInfo = additionalInfo;
  }

  public void setAdditionalInfo(String key, Object value) {
    if (additionalInfo == null) {
      additionalInfo = new HashMap<>();
    }
    additionalInfo.put(key, value);
  }

  public Map<String, Object> getAdditionalInfo() {
    return additionalInfo;
  }

  // used by test cases (getErrorCode before)
  public String retrieveErrorCode() {
    return ((ErrorMessage)message).getErrorCode();
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getConfigGroup() {
    return configGroup;
  }

  public String getConfigName() {
    return configName;
  }

  // used by (getLevel() by container json)
  public String retrieveLevel() {
    String level;
    if (instanceName == null) {
      level = (getConfigName() == null) ? "PIPELINE" : "PIPELINE_CONFIG";
    } else {
      level = (getConfigName() == null) ? "STAGE" : "STAGE_CONFIG";
    }
    return level;
  }

  @Override
  public String toString() {
    return Utils.format("Issue[instance='{}' group='{}' config='{}' message='{}']", instanceName, configGroup,
                        configName, message.getNonLocalized());
  }

}
