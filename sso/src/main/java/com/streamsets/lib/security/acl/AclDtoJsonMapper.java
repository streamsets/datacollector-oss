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
package com.streamsets.lib.security.acl;

import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;
import com.streamsets.lib.security.acl.json.AclJson;
import com.streamsets.lib.security.acl.json.ActionJson;
import com.streamsets.lib.security.acl.json.PermissionJson;
import com.streamsets.lib.security.acl.json.ResourceTypeJson;
import com.streamsets.lib.security.acl.json.SubjectTypeJson;
import fr.xebia.extras.selma.Mapper;
import fr.xebia.extras.selma.Selma;

import java.util.List;

@Mapper
public abstract class AclDtoJsonMapper {
  public static final AclDtoJsonMapper INSTANCE = Selma.builder(AclDtoJsonMapper.class).build();

  public abstract AclJson toAclJson(Acl acl);

  public abstract Acl asAclDto(AclJson aclJson);

  public abstract Acl cloneAcl(Acl acl);

  public abstract ActionJson toActionJson(Action action);

  public abstract Action asActionDto(ActionJson actionJson);

  public abstract PermissionJson toPermissionJson(Permission permission);

  public abstract Permission asPermissionDto(PermissionJson permissionJson);

  public abstract List<PermissionJson> toPermissionsJson(List<Permission> permission);

  public abstract List<Permission> asPermissionsDto(List<PermissionJson> permissionJson);

  public abstract ResourceTypeJson toResourceTypeJson(ResourceType resourceType);

  public abstract ResourceType asResourceTypeDto(ResourceTypeJson resourceTypeJson);

  public abstract SubjectTypeJson toSubjectTypeJson(SubjectType subjectType);

  public abstract SubjectType asSubjectTypeDto(SubjectTypeJson subjectTypeJson);
}
