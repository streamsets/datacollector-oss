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

public class Acl {
  private String resourceId;
  private String resourceOwner;
  private long resourceCreatedTime;
  private ResourceType resourceType;
  private String lastModifiedBy;
  private long lastModifiedOn;
  private List<Permission> permissions;

  public Acl(
      String resourceId,
      String resourceOwner,
      long resourceCreatedTime,
      ResourceType resourceType,
      String lastModifiedBy,
      long lastModifiedOn,
      List<Permission> permissions
  ) {
    this.resourceId = resourceId;
    this.resourceOwner = resourceOwner;
    this.resourceType = resourceType;
    this.resourceCreatedTime = resourceCreatedTime;
    this.lastModifiedBy = lastModifiedBy;
    this.lastModifiedOn = lastModifiedOn;
    this.permissions = permissions;
  }

  public Acl() {
    this.permissions = new ArrayList<>();
  }

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public String getResourceOwner() {
    return resourceOwner;
  }

  public void setResourceOwner(String resourceOwner) {
    this.resourceOwner = resourceOwner;
  }

  public long getResourceCreatedTime() {
    return resourceCreatedTime;
  }

  public void setResourceCreatedTime(long resourceCreatedTime) {
    this.resourceCreatedTime = resourceCreatedTime;
  }

  public ResourceType getResourceType() {
    return resourceType;
  }

  public void setResourceType(ResourceType resourceType) {
    this.resourceType = resourceType;
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

  public List<Permission> getPermissions() {
    return permissions;
  }

  public void setPermissions(List<Permission> permissions) {
    this.permissions = permissions;
  }
}
