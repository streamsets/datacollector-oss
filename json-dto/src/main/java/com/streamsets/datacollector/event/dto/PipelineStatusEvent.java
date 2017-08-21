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
package com.streamsets.datacollector.event.dto;

import java.util.Collection;
import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.lib.security.acl.dto.Acl;

public class PipelineStatusEvent implements Event {

  private String name;
  private String title;
  private String rev;
  private long timeStamp;
  private PipelineStatus pipelineStatus;
  private String message;
  private ValidationStatus validationStatus;
  private String issues;
  private Collection<WorkerInfo> workerInfos;
  private boolean isRemote;
  private boolean isClusterMode;
  private String offset;
  private int offsetProtocolVersion;
  private Acl acl;
  private int runnerCount;

  public PipelineStatusEvent() {
  }

  public PipelineStatusEvent(
      String name,
      String title,
      String rev,
      long timeStamp,
      boolean isRemote,
      PipelineStatus pipelineStatus,
      String message,
      Collection<WorkerInfo> workerInfos,
      ValidationStatus validationStatus,
      String issues,
      boolean isClusterMode,
      String offset,
      int offsetProtocolVersion,
      Acl acl,
      int runnerCount
  ) {
    this.name = name;
    this.title = title;
    this.rev = rev;
    this.timeStamp = timeStamp;
    this.pipelineStatus = pipelineStatus;
    this.message = message;
    this.validationStatus = validationStatus;
    this.issues = issues;
    this.isRemote = isRemote;
    this.workerInfos = workerInfos;
    this.isClusterMode = isClusterMode;
    this.offset = offset;
    this.acl = acl;
    this.offsetProtocolVersion = offsetProtocolVersion;
    this.runnerCount = runnerCount;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public void setRemote(boolean isRemote) {
    this.isRemote = isRemote;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getRev() {
    return rev;
  }

  public void setRev(String rev) {
    this.rev = rev;
  }

  public PipelineStatus getPipelineStatus() {
    return pipelineStatus;

  }

  public void setPipelineStatus(PipelineStatus pipelineStatus) {
    this.pipelineStatus = pipelineStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ValidationStatus getValidationStatus() {
    return validationStatus;
  }

  public void setValidationStatus(ValidationStatus validationStatus) {
    this.validationStatus = validationStatus;
  }

  public String getIssues() {
    return issues;
  }

  public void setIssues(String issues) {
    this.issues = issues;
  }

  public void setWorkerInfos(Collection<WorkerInfo> workerInfos) {
    this.workerInfos = workerInfos;
  }

  public Collection<WorkerInfo> getWorkerInfos() {
    return workerInfos;
  }

  public boolean isClusterMode() {
    return isClusterMode;
  }

  public void setClusterMode(boolean clusterMode) {
    isClusterMode = clusterMode;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public void setAcl(Acl acl) {
    this.acl = acl;
  }

  public Acl getAcl() {
    return acl;
  }

  public int getOffsetProtocolVersion() {
    return offsetProtocolVersion;
  }

  public void setOffsetProtocolVersion(int offsetProtocolVersion) {
    this.offsetProtocolVersion = offsetProtocolVersion;
  }

  public int getRunnerCount() {
    return runnerCount;
  }

  public void setRunnerCount(int runnerCount) {
    this.runnerCount = runnerCount;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }
}
