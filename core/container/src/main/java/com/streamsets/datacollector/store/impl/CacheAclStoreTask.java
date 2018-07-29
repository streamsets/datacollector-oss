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

import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.ResourceType;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheAclStoreTask extends AbstractAclStoreTask {
  private final AclStoreTask aclStore;
  private final ConcurrentMap<String, Acl> pipelineAclMap;
  private final LockCache<String> lockCache;

  @Inject
  public CacheAclStoreTask(
      AclStoreTask aclStore,
      PipelineStoreTask pipelineStore,
      LockCache<String> lockCache,
      UserGroupManager userGroupManager
  ) {
    super(pipelineStore, lockCache, userGroupManager);
    this.aclStore = aclStore;
    pipelineAclMap = new ConcurrentHashMap<>();
    this.lockCache = lockCache;
  }

  @Override
  public String getName() {
    return "CacheAclStoreTask";
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    aclStore.waitWhileRunning();
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
    synchronized (lockCache.getLock(name)) {
      Acl acl = aclStore.createAcl(name, resourceType, resourceCreateTime, resourceOwner);
      pipelineAclMap.put(name, acl);
      return acl;
    }
  }

  @Override
  public Acl saveAcl(String name, Acl acl) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      aclStore.saveAcl(name, acl);
      pipelineAclMap.put(name, acl);
      return acl;
    }
  }

  @Override
  public Acl getAcl(String name) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      if (!pipelineAclMap.containsKey(name)) {
        Acl acl = aclStore.getAcl(name);
        if (acl != null) {
          pipelineAclMap.put(name, acl);
        }
      }
      return pipelineAclMap.get(name);
    }
  }

  @Override
  public void deleteAcl(String name) {
    synchronized (lockCache.getLock(name)) {
      pipelineAclMap.remove(name);
    }
  }
}
