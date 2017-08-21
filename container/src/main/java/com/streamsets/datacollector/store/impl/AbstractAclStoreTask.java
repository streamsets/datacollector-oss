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
package com.streamsets.datacollector.store.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractAclStoreTask extends AbstractTask implements AclStoreTask {
  private final PipelineStoreTask pipelineStore;
  private final LockCache<String> lockCache;
  private final UserGroupManager userGroupManager;

  AbstractAclStoreTask(
      PipelineStoreTask pipelineStoreTask,
      LockCache<String> lockCache,
      UserGroupManager userGroupManager
  ) {
    super("aclStore");
    this.pipelineStore = pipelineStoreTask;
    this.lockCache = lockCache;
    this.userGroupManager = userGroupManager;
  }

  @Override
  public void validateReadPermission(String pipelineName, UserJson currentUser) throws PipelineException {
    if (!isPermissionGranted(pipelineName, EnumSet.of(Action.READ), currentUser)) {
      throw new PipelineException(
          ContainerError.CONTAINER_01200,
          SubjectType.USER,
          currentUser.getName(),
          Action.READ,
          pipelineName
      );
    }
  }

  @Override
  public void validateWritePermission(String pipelineName, UserJson currentUser) throws PipelineException {
    if (!isPermissionGranted(pipelineName, EnumSet.of(Action.WRITE), currentUser)) {
      throw new PipelineException(
          ContainerError.CONTAINER_01200,
          SubjectType.USER,
          currentUser.getName(),
          Action.WRITE,
          pipelineName
      );
    }
  }

  @Override
  public void validateExecutePermission(String pipelineName, UserJson currentUser) throws PipelineException {
    if (!isPermissionGranted(pipelineName, EnumSet.of(Action.EXECUTE), currentUser)) {
      throw new PipelineException(
          ContainerError.CONTAINER_01200,
          SubjectType.USER,
          currentUser.getName(),
          Action.EXECUTE,
          pipelineName
      );
    }
  }

  @Override
  public boolean isPermissionGranted(
      String pipelineName,
      Set<Action> actions,
      UserJson currentUser
  ) throws PipelineException {
    if (currentUser == null || isUserAdmin(currentUser)) {
      return true;
    }
    Acl acl = getAcl(pipelineName);
    if (acl == null) {
      // For old pipelines for which there is no acl.json return true for pipeline owner
      PipelineInfo pipelineInfo = pipelineStore.getInfo(pipelineName);
      return pipelineInfo.getCreator().equals(currentUser.getName());
    }
    return isPermissionGranted(acl, actions, currentUser);
  }

  @Override
  public void updateSubjectsInAcls(Map<String, String> subjectToSubjectMapping) throws PipelineException {
    for (PipelineInfo pipelineInfo : pipelineStore.getPipelines()) {
      String pipelineName = pipelineInfo.getPipelineId();
      synchronized (lockCache.getLock(pipelineName)) {
        updateSubjectsInAcls(pipelineName, subjectToSubjectMapping);
      }
    }
  }

  private void updateSubjectsInAcls(
      String pipelineName,
      Map<String, String> subjectToSubjectMapping
  ) throws PipelineException {
    Acl acl = getAcl(pipelineName);
    if (acl == null) {
      // If ACL file doesn't exist create default one
      PipelineInfo pipelineInfo = pipelineStore.getInfo(pipelineName);
      acl = createAcl(
          pipelineInfo.getPipelineId(),
          ResourceType.PIPELINE,
          pipelineInfo.getCreated().getTime(),
          pipelineInfo.getCreator()
      );
    }

    String resourceOwner = acl.getResourceOwner();
    String newResourceOwner = subjectToSubjectMapping.get(resourceOwner);
    //No mapping defined
    if (newResourceOwner != null) {
      // Only users can be owner of the pipeline
      if (!userGroupManager.getGroups().contains(newResourceOwner)) {
        acl.setResourceOwner(newResourceOwner);
      }
    }
    for (Permission permission : acl.getPermissions()) {
      if (permission != null) {
        String lastModifiedBy = permission.getLastModifiedBy();
        String newModifiedBy = subjectToSubjectMapping.get(lastModifiedBy);
        //No mapping defined
        if (newModifiedBy != null) {
          permission.setLastModifiedBy(newModifiedBy);
        }
        String subjectId = permission.getSubjectId();
        String newSubjectId = subjectToSubjectMapping.get(subjectId);
        //No mapping defined
        if (newSubjectId != null) {
          permission.setSubjectId(newSubjectId);
          if (userGroupManager.getGroups().contains(newSubjectId)) {
            permission.setSubjectType(SubjectType.GROUP);
          } else {
            permission.setSubjectType(SubjectType.USER);
          }
        }
      }
    }
    saveAcl(pipelineName, acl);
  }

  private boolean isPermissionGranted(Acl acl, Set<Action> actions, UserJson currentUser) {
    boolean permissionGranted = false;
    List<String> subjectIds = new ArrayList<>();
    subjectIds.add(currentUser.getName());
    if (currentUser.getGroups() != null) {
      subjectIds.addAll(currentUser.getGroups());
    }
    Collection<Permission> permissions = filterPermission(acl, subjectIds);
    for (Permission permission : permissions) {
      permissionGranted = permission != null && permission.getActions().containsAll(actions);
      if (permissionGranted) {
        break;
      }
    }
    return permissionGranted;
  }

  private boolean isUserAdmin(UserJson user) {
    return user.getRoles() != null &&
        (user.getRoles().contains(AuthzRole.ADMIN) || user.getRoles().contains(AuthzRole.ADMIN_REMOTE));
  }

  private Collection<Permission> filterPermission(Acl acl, final List<String> subjectIds) {
    return Collections2.filter(acl.getPermissions(), new Predicate<Permission>() {
      @Override
      public boolean apply(Permission permission) {
        return subjectIds.contains(permission.getSubjectId());
      }
    });
  }

  @Override
  public Map<String, Set<String>> getSubjectsInAcls() throws PipelineException {
    Map<String, Set<String>> subjects = new HashMap<>();
    Set<String> users = new HashSet<>();
    Set<String> groups = new HashSet<>();
    for (PipelineInfo pipelineInfo : pipelineStore.getPipelines()) {
      Acl acl = getAcl(pipelineInfo.getPipelineId());
      if (acl != null) {
        users.add(acl.getResourceOwner());
        for (Permission permission: acl.getPermissions()) {
          if (permission.getSubjectType() == SubjectType.GROUP) {
            groups.add(permission.getSubjectId());
          } else {
            users.add(permission.getSubjectId());
          }
        }
      }
    }
    subjects.put("groups", groups);
    subjects.put("users", users);
    return subjects;
  }

}
