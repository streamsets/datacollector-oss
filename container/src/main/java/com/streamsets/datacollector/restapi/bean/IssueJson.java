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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IssueJson {

  private final Issue issue;

  @JsonCreator
  public IssueJson(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("serviceName") String serviceName,
      @JsonProperty("level") String level,
      @JsonProperty("configGroup") String configGroup,
      @JsonProperty("configName") String configName,
      @JsonProperty("message") String message,
      @JsonProperty("count") long count,
      @JsonProperty("additionalInfo") Map<String, Object> additionalInfo,
      @JsonProperty("antennaDoctorMessages") List<AntennaDoctorMessageJson> antennaDoctorMessages
  ) {
    String errorCode = "";
    String errorMessage = "";
    if (StringUtils.isNotEmpty(message)) {
      String[] messageArr = message.split(" - ", 2);
      if (messageArr.length > 1) {
        errorCode = messageArr[0];
        errorMessage = messageArr[1];
      }
    }
    this.issue = new Issue(
        instanceName,
        serviceName,
        configGroup,
        configName,
        count,
        new ErrorMessage(errorCode, errorMessage, System.currentTimeMillis()),
        additionalInfo,
        BeanHelper.unwrapAntennaDoctorMessages(antennaDoctorMessages)
    );
  }

  IssueJson(Issue issue) {
    this.issue = issue;
  }

  public String getInstanceName() {
    return issue.getInstanceName();
  }

  public String getServiceName() {
    return issue.getServiceName();
  }

  public String getLevel() {
    return issue.getLevel();
  }

  public String getConfigGroup() {
    return issue.getConfigGroup();
  }

  public String getConfigName() {
    return issue.getConfigName();
  }

  public String getMessage() { return issue.getMessage();
  }

  public long getCount() {
    return issue.getCount();
  }

  public Map getAdditionalInfo() {
    return issue.getAdditionalInfo();
  }

  @JsonIgnore
  public Issue getIssue() {
    return issue;
  }

  public List<AntennaDoctorMessageJson> getAntennaDoctorMessages() {
    return BeanHelper.wrapAntennaDoctorMessages(issue.getAntennaDoctorMessages());
  }
}
