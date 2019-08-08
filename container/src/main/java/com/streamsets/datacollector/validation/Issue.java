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

import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.validation.ValidationIssue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Issue implements Serializable, ValidationIssue {
  private final String instanceName;
  private final String serviceName;
  private final String configGroup;
  private final String configName;
  private long count;
  private LocalizableString message;
  private Map<String, Object> additionalInfo;
  private List<AntennaDoctorMessage> antennaDoctorMessages;

  protected Issue(String instanceName, String serviceName, String configGroup, String configName, ErrorCode error, Object... args) {
    this.instanceName = instanceName;
    this.serviceName = serviceName;
    this.configGroup = configGroup;
    this.configName = configName;
    this.count = 1;
    message = new ErrorMessage(error, args);
  }

  public Issue(
      String instanceName,
      String serviceName,
      String configGroup,
      String configName,
      long count,
      LocalizableString message,
      Map<String, Object> additionalInfo,
      List<AntennaDoctorMessage> antennaDoctorMessages
  ) {
    this.instanceName = instanceName;
    this.serviceName = serviceName;
    this.configGroup = configGroup;
    this.configName = configName;
    this.count = count;
    this.message = message;
    this.additionalInfo = additionalInfo;
    this.antennaDoctorMessages = antennaDoctorMessages;
  }

  public void setAdditionalInfo(String key, Object value) {
    if (additionalInfo == null) {
      additionalInfo = new HashMap<>();
    }
    additionalInfo.put(key, value);
  }

  public void setMessage(LocalizableString message) {
    this.message = message;
  }

  @Override
  public Map getAdditionalInfo() {
    return additionalInfo;
  }

  @Override
  public String getMessage() {
    return message.getLocalized();
  }

  @Override
  public String getErrorCode() {
    return ((ErrorMessage)message).getErrorCode();
  }

  @Override
  public String getInstanceName() {
    return instanceName;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  @Override
  public String getLevel() {
    String level;
    if (instanceName == null) {
      level = (getConfigName() == null) ? "PIPELINE" : "PIPELINE_CONFIG";
    } else {
      level = (getConfigName() == null) ? "STAGE" : "STAGE_CONFIG";
    }
    return level;
  }

  @Override
  public String getConfigGroup() {
    return configGroup;
  }

  @Override
  public String getConfigName() {
    return configName;
  }

  @Override
  public String toString() {
    return Utils.format("Issue[instance='{}' service='{}' group='{}' config='{}' message='{}']",
      instanceName,
      serviceName,
      configGroup,
      configName,
      message.getNonLocalized()
    );
  }

  public long getCount() {
    return count;
  }

  public void incCount() {
    count++;
  }

  public void setAntennaDoctorMessages(List<AntennaDoctorMessage> messages) {
    this.antennaDoctorMessages = messages;
  }

  public List<AntennaDoctorMessage> getAntennaDoctorMessages() {
    return antennaDoctorMessages;
  }
}
