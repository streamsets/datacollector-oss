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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TestAclCacheHelper {

  @Test
  public void testGetAclCache() throws Exception {
    PipelineStoreTask pipelineStoreTask = Mockito.mock(PipelineStoreTask.class);
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);

    AclCacheHelper aclCacheHelper = new AclCacheHelper(new Configuration(), pipelineStoreTask, aclStoreTask);
    Acl acl = new Acl();
    acl.setResourceId("resource");
    acl.setResourceType(ResourceType.PIPELINE);
    Permission permission0 = new Permission();
    permission0.setSubjectId("foo");
    permission0.setSubjectType(SubjectType.USER);
    permission0.setActions(ImmutableList.of(Action.READ, Action.WRITE));
    acl.setPermissions(ImmutableList.of(permission0));
    Mockito.when(aclStoreTask.getAcl(acl.getResourceId())).thenReturn(acl);
    Acl gotAcl = aclCacheHelper.getAcl(acl.getResourceId());
    Assert.assertNotNull(gotAcl);
    Assert.assertEquals(acl.getResourceId(), gotAcl.getResourceId());
    Assert.assertEquals(acl.getResourceType(), gotAcl.getResourceType());
    Assert.assertNull(acl.getLastModifiedBy());
    Assert.assertEquals(permission0.getSubjectId(), gotAcl.getPermissions().get(0).getSubjectId());
    Assert.assertEquals(permission0.getSubjectType(), gotAcl.getPermissions().get(0).getSubjectType());
    Assert.assertEquals(2, gotAcl.getPermissions().get(0).getActions().size());
    Assert.assertEquals(ImmutableList.of(Action.READ, Action.WRITE), gotAcl.getPermissions().get(0).getActions());

    Mockito.when(aclStoreTask.getAcl(acl.getResourceId())).thenReturn(acl);
    // no change so return acl should be null
    gotAcl = aclCacheHelper.getAcl(acl.getResourceId());
    Assert.assertNull(gotAcl);

    // change acl, set some attribute and modify action in existing permission bean
    acl.setLastModifiedBy("foo");
    permission0.setActions(ImmutableList.of(Action.READ, Action.EXECUTE));
    acl.setPermissions(ImmutableList.of(permission0));
    Mockito.when(aclStoreTask.getAcl(acl.getResourceId())).thenReturn(acl);
    gotAcl = aclCacheHelper.getAcl(acl.getResourceId());
    Assert.assertNotNull(gotAcl);
    Assert.assertEquals(acl.getResourceId(), gotAcl.getResourceId());
    Assert.assertEquals(acl.getResourceType(), gotAcl.getResourceType());
    Assert.assertEquals(acl.getLastModifiedBy(), gotAcl.getLastModifiedBy());
    Assert.assertEquals(permission0.getSubjectId(), gotAcl.getPermissions().get(0).getSubjectId());
    Assert.assertEquals(permission0.getSubjectType(), gotAcl.getPermissions().get(0).getSubjectType());
    Assert.assertEquals(2, gotAcl.getPermissions().get(0).getActions().size());
    Assert.assertEquals(ImmutableList.of(Action.READ, Action.EXECUTE), gotAcl.getPermissions().get(0).getActions());

    // change acl by adding one more permission
    Permission permission1 = new Permission();
    permission1.setSubjectId("foo2");
    permission1.setSubjectType(SubjectType.USER);
    permission1.setActions(ImmutableList.of(Action.READ, Action.WRITE, Action.EXECUTE));
    acl.setPermissions(ImmutableList.of(permission0, permission1));
    Mockito.when(aclStoreTask.getAcl(acl.getResourceId())).thenReturn(acl);
    gotAcl = aclCacheHelper.getAcl(acl.getResourceId());
    Assert.assertNotNull(gotAcl);
    Assert.assertEquals(2, gotAcl.getPermissions().size());
    Assert.assertEquals(permission0.getSubjectId(), gotAcl.getPermissions().get(0).getSubjectId());
    Assert.assertEquals(permission1.getSubjectId(), gotAcl.getPermissions().get(1).getSubjectId());

    Mockito.when(aclStoreTask.getAcl(acl.getResourceId())).thenReturn(null);
    gotAcl = aclCacheHelper.getAcl(acl.getResourceId());
    Assert.assertNotNull(gotAcl);
    Assert.assertEquals(2, gotAcl.getPermissions().size());
    Assert.assertEquals(permission0.getSubjectId(), gotAcl.getPermissions().get(0).getSubjectId());
    Assert.assertEquals(permission1.getSubjectId(), gotAcl.getPermissions().get(1).getSubjectId());

    String newResource = "someRandom";
    Mockito.when(aclStoreTask.getAcl(newResource)).thenReturn(null);
    Date createdDate = new Date(0);
    PipelineInfo pipelineInfo = new PipelineInfo(newResource,
        "label",
        "description",
        createdDate,
        createdDate,
        "creator",
        "lastModifier",
        "1",
        UUID.randomUUID(),
        true,
        null,
        "2.5",
        "x"
    );
    Mockito.when(pipelineStoreTask.getInfo(newResource)).thenReturn(pipelineInfo);
    gotAcl = aclCacheHelper.getAcl(newResource);
    Assert.assertNotNull(gotAcl);
    Assert.assertEquals(newResource, gotAcl.getResourceId());
    Assert.assertEquals("creator", gotAcl.getResourceOwner());
    Assert.assertEquals(createdDate.getTime(), gotAcl.getResourceCreatedTime());
    Assert.assertEquals(1, gotAcl.getPermissions().size());
    Assert.assertEquals(gotAcl.getResourceOwner(), gotAcl.getPermissions().get(0).getSubjectId());
    Assert.assertEquals(SubjectType.USER, gotAcl.getPermissions().get(0).getSubjectType());

    Acl sameAclObject = aclCacheHelper.getAcl(newResource);
    Assert.assertEquals(gotAcl, sameAclObject);
  }

  @Test
  public void testRemoveIfAbsent() throws Exception {
    PipelineStoreTask pipelineStoreTask = Mockito.mock(PipelineStoreTask.class);
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);

    AclCacheHelper aclCacheHelper = new AclCacheHelper(new Configuration(), pipelineStoreTask, aclStoreTask);
    String resource1 = "resource1";
    Mockito.when(aclStoreTask.getAcl(resource1)).thenReturn(null);
    Date createdDate = new Date(0);
    PipelineInfo pipelineInfo = new PipelineInfo(resource1,
        "label",
        "description",
        createdDate,
        createdDate,
        "creator",
        "lastModifier",
        "1",
        UUID.randomUUID(),
        true,
        null,
        "2.5",
        "x"
    );
    Mockito.when(pipelineStoreTask.getInfo(resource1)).thenReturn(pipelineInfo);
    Assert.assertNotNull(aclCacheHelper.getAcl(resource1));
    String resource2 = "resource2";
    PipelineInfo pipelineInfo2 = new PipelineInfo(resource2,
        "label",
        "description",
        createdDate,
        createdDate,
        "creator",
        "lastModifier",
        "1",
        UUID.randomUUID(),
        true,
        null,
        "2.5",
        "x"
    );
    Mockito.when(pipelineStoreTask.getInfo(resource2)).thenReturn(pipelineInfo2);
    Assert.assertNotNull(aclCacheHelper.getAcl(resource2));

    Set<String> removedIds = aclCacheHelper.removeIfAbsent(new HashSet<>(Arrays.asList(resource1)));
    Assert.assertEquals(resource2, removedIds.iterator().next());
    removedIds = aclCacheHelper.removeIfAbsent(Collections.<String>emptySet());
    Assert.assertEquals(resource1, removedIds.iterator().next());
  }
}
