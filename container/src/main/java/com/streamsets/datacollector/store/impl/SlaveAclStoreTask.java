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

import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.ResourceType;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

public class SlaveAclStoreTask implements AclStoreTask {
  private final AclStoreTask aclStore;

  @Inject
  public SlaveAclStoreTask(AclStoreTask aclStore) {
    this.aclStore = aclStore;
  }

  @Override
  public String getName() {
    return "SlaveAclStoreTask";
  }

  @Override
  public void init() {
    aclStore.init();
  }

  @Override
  public void run() {
    aclStore.run();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    aclStore.waitWhileRunning();
  }

  @Override
  public void stop() {
    aclStore.stop();
  }

  @Override
  public Status getStatus() {
    return aclStore.getStatus();
  }

  @Override
  public Acl createAcl(
      String name,
      ResourceType resourceType,
      long resourceCreateTime,
      String resourceOwner
  ) throws PipelineStoreException {
    return aclStore.createAcl(name, resourceType, resourceCreateTime, resourceOwner);
  }

  @Override
  public Acl saveAcl(String name, Acl acl) throws PipelineException {
    return aclStore.saveAcl(name, acl);
  }

  @Override
  public Acl getAcl(String name) throws PipelineException {
    return aclStore.getAcl(name);
  }

  @Override
  public void deleteAcl(String name) {
    aclStore.deleteAcl(name);
  }

  @Override
  public void validateReadPermission(String pipelineName, UserJson currentUser) throws PipelineException {
    aclStore.validateReadPermission(pipelineName, currentUser);
  }

  @Override
  public void validateWritePermission(String pipelineName, UserJson currentUser) throws PipelineException {
    aclStore.validateWritePermission(pipelineName, currentUser);
  }

  @Override
  public void validateExecutePermission(String pipelineName, UserJson currentUser) throws PipelineException {
    aclStore.validateExecutePermission(pipelineName, currentUser);
  }

  @Override
  public boolean isPermissionGranted(
      String pipelineName,
      Set<Action> actions,
      UserJson currentUser
  ) throws PipelineException {
    return aclStore.isPermissionGranted(pipelineName, actions, currentUser);
  }

  @Override
  public void updateSubjectsInAcls(Map<String, String> subjectToSubjectMapping) throws PipelineException {
    throw new UnsupportedOperationException("updateSubjectsInAcls is not allowed in Slave Mode");
  }

  @Override
  public Map<String, Set<String>> getSubjectsInAcls() throws PipelineException {
    throw new UnsupportedOperationException("getSubjectsInAcls is not allowed in Slave Mode");
  }
}
