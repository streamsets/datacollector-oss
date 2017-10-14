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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.datacollector.util.NullDeserializer;
import com.streamsets.datacollector.validation.Issue;

import java.util.Map;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class IssueJson {

  private final Issue issue;

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
}
