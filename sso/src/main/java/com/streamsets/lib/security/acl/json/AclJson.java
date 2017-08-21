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

public class AclJson {
  private String resourceId;
  private String resourceOwner;
  private long resourceCreatedTime;
  private ResourceTypeJson resourceType;
  private String lastModifiedBy;
  private long lastModifiedOn;
  private List<PermissionJson> permissions = new ArrayList<>();

  public AclJson() {
    permissions = new ArrayList<>();
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

  public ResourceTypeJson getResourceType() {
    return resourceType;
  }

  public void setResourceType(ResourceTypeJson resourceType) {
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

  public List<PermissionJson> getPermissions() {
    return permissions;
  }

  public void setPermissions(List<PermissionJson> permissions) {
    this.permissions = permissions;
  }
}
