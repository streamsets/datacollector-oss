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
package com.streamsets.lib.security.acl.dto;

import java.util.ArrayList;
import java.util.List;

public class Permission {
  private String subjectId;
  private SubjectType subjectType;
  private String lastModifiedBy;
  private long lastModifiedOn;
  private List<Action> actions;

  public Permission(
      String subjectId, SubjectType subjectType, String lastModifiedBy, long lastModifiedOn, List<Action> actions
  ) {
    this.subjectId = subjectId;
    this.subjectType = subjectType;
    this.lastModifiedBy = lastModifiedBy;
    this.lastModifiedOn = lastModifiedOn;
    this.actions = actions;
  }

  public Permission() {
    this.actions = new ArrayList<>();
  }

  public String getSubjectId() {
    return subjectId;
  }

  public void setSubjectId(String subjectId) {
    this.subjectId = subjectId;
  }

  public SubjectType getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(SubjectType subjectType) {
    this.subjectType = subjectType;
  }

  public String getLastModifiedBy() {
    return lastModifiedBy;
  }

  public void setLastModifiedBy(String lastModifiedBy) {
    this.lastModifiedBy = lastModifiedBy;
  }

  public long getLastModifiedOn() {
    return lastModifiedOn;
  }

  public void setLastModifiedOn(long lastModifiedOn) {
    this.lastModifiedOn = lastModifiedOn;
  }

  public List<Action> getActions() {
    return actions;
  }

  public void setActions(List<Action> actions) {
    this.actions = actions;
  }
}
