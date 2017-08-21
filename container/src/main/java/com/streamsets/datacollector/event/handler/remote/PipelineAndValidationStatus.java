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
package com.streamsets.datacollector.event.handler.remote;

import java.util.Collection;

import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.event.dto.WorkerInfo;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.lib.security.acl.dto.Acl;

public class PipelineAndValidationStatus {
  private final String name;
  private final String title;
  private final String rev;
  private final long timeStamp;
  private final PipelineStatus pipelineStatus;
  private final boolean isRemote;
  private ValidationStatus validationStatus;
  private Issues issues;
  private String message;
  private Collection<WorkerInfo> workerInfos;
  private boolean isClusterMode;
  private String offset;
  private Acl acl;
  private int runnerCount;

  public PipelineAndValidationStatus(
      String name,
      String title,
      String rev,
      long timeStamp,
      boolean isRemote,
      PipelineStatus pipelineStatus,
      String message,
      Collection<WorkerInfo> workerInfos,
      boolean isClusterMode,
      String offset,
      Acl acl,
      int runnerCount
  ) {
    this.name = name;
    this.title = title;
    this.rev = rev;
    this.isRemote = isRemote;
    this.pipelineStatus = pipelineStatus;
    this.message = message;
    this.workerInfos = workerInfos;
    this.isClusterMode = isClusterMode;
    this.offset = offset;
    this.acl = acl;
    this.runnerCount = runnerCount;
    this.timeStamp = timeStamp;
  }

  public void setValidationStatus(ValidationStatus validationStatus) {
    this.validationStatus = validationStatus;
  }

  public void setIssues(Issues issues) {
    this.issues = issues;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ValidationStatus getValidationStatus() {
    return validationStatus;
  }

  public Collection<WorkerInfo> getWorkerInfos() {
    return workerInfos;
  }

  public Issues getIssues() {
    return issues;
  }

  public String getMessage() {
    return message;
  }

  public String getName() {
    return name;
  }

  public String getTitle() {
    return title;
  }

  public String getRev() {
    return rev;
  }

  public PipelineStatus getPipelineStatus() {
    return pipelineStatus;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public boolean isClusterMode() {
    return isClusterMode;
  }

  public String getOffset() {
    return offset;
  }

  public Acl getAcl() {
    return acl;
  }

  public int getRunnerCount() {
    return runnerCount;
  }

  public long getTimeStamp() {
    return timeStamp;
  }
}

