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
package com.streamsets.lib.security.acl.json;

import java.util.ArrayList;
import java.util.List;

public class PermissionJson {

  private String subjectId;
  private SubjectTypeJson subjectType;
  private String lastModifiedBy;
  private long lastModifiedOn;
  private List<ActionJson> actions = new ArrayList<>();

  public String getSubjectId() {
    return subjectId;
  }

  public void setSubjectId(String subjectId) {
    this.subjectId = subjectId;
  }

  public SubjectTypeJson getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(SubjectTypeJson subjectType) {
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

  public List<ActionJson> getActions() {
    return actions;
  }

  public void setActions(List<ActionJson> actions) {
    this.actions = actions;
  }
}
