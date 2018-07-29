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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.AclDtoJsonMapper;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.SubjectType;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AclCacheHelper {

  private Cache<String, Acl> cache;
  private static final String REMOTE_EVENTS_ACL_CACHE_SIZE = "remote.events.acl.cache.size";
  private static final long DEFAULT_REMOTE_EVENTS_ACL_CACHE_SIZE = 1000;
  private static final ObjectMapper objectMapper = ObjectMapperFactory.get();
  private final PipelineStoreTask pipelineStoreTask;
  private final AclStoreTask aclStoreTask;

  @Inject
  public AclCacheHelper(Configuration conf, PipelineStoreTask pipelineStoreTask, AclStoreTask aclStoreTask) {
    cache = CacheBuilder.newBuilder().maximumSize(conf.get(REMOTE_EVENTS_ACL_CACHE_SIZE,
        DEFAULT_REMOTE_EVENTS_ACL_CACHE_SIZE
    )).build();
    this.pipelineStoreTask = pipelineStoreTask;
    this.aclStoreTask = aclStoreTask;
  }

  public Acl getAcl(String resourceId) throws PipelineException, IOException {
    Acl acl = aclStoreTask.getAcl(resourceId);
    if (acl != null) {
      //if acl changed, send it otherwise null
      acl = getIfChangedOrNull(resourceId, acl);
    } else {
      // send acl with owner permissions when no acl file
      acl = getOrCreate(
          resourceId,
          pipelineStoreTask.getInfo(resourceId)
      );
    }
    return acl;
  }

  private Acl getIfChangedOrNull(String resourceId, Acl actualAcl) throws IOException {
    boolean isChanged = true;
    Acl acl = cache.getIfPresent(resourceId);
    if (acl != null && acl.getPermissions().size() == actualAcl.getPermissions().size()) {
      if (objectMapper.writeValueAsString(actualAcl).equals(objectMapper.writeValueAsString(acl))) {
        isChanged = false;
      }
    }
    Acl changedAcl = null;
    if (isChanged) {
      changedAcl = AclDtoJsonMapper.INSTANCE.cloneAcl(actualAcl);
      cache.put(resourceId, changedAcl);
    }
    return changedAcl;
  }

  private Acl getOrCreate(String resourceId, PipelineInfo pipelineInfo) {
    Acl acl = cache.getIfPresent(resourceId);
    if (acl != null) {
      return acl;
    }
    acl = new Acl();
    acl.setResourceId(resourceId);
    acl.setResourceOwner(pipelineInfo.getCreator());
    acl.setResourceCreatedTime(pipelineInfo.getCreated().getTime());
    Permission permission = new Permission();
    permission.setSubjectId(pipelineInfo.getCreator());
    permission.setSubjectType(SubjectType.USER);
    permission.setActions(ImmutableList.of(Action.READ, Action.WRITE, Action.EXECUTE));
    acl.setPermissions(ImmutableList.of(permission));
    cache.put(resourceId, acl);
    return acl;
  }

  public Set<String> removeIfAbsent(Set<String> localPipelineIds) {
    Set<String> removedIds = new HashSet<>();
    for (String resource: cache.asMap().keySet()) {
      if (!localPipelineIds.contains(resource)) {
        cache.invalidate(resource);
        removedIds.add(resource);
      }
    }
    return removedIds;
  }
}
