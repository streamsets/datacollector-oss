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

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Action;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestFileAclStoreTask {
  protected PipelineStoreTask store;
  protected AclStoreTask aclStore;

  @dagger.Module(injects = {PipelineStoreTask.class, FileAclStoreTask.class, LockCache.class, UserGroupManager.class},
      includes = LockCacheModule.class)
  public static class Module {
    public Module() {
    }
    @Provides
    @Singleton
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getDataDir()).thenReturn("target/" + UUID.randomUUID());
      return mock;
    }

    @Provides
    @Singleton
    public StageLibraryTask provideStageLibrary() {
      return MockStages.createStageLibrary();
    }

    @Provides
    @Singleton
    @Nullable
    public PipelineStateStore providePipelineStateStore() {
      return null;
    }

    @Provides
    @Singleton
    @Nullable
    public UserGroupManager provideUserGroupManager() {
      return Mockito.mock(UserGroupManager.class);
    }

    @Provides
    @Singleton
    public PipelineStoreTask providePipelineStoreTask(
        RuntimeInfo runtimeInfo,
        StageLibraryTask stageLibraryTask,
        PipelineStateStore pipelineStateStore,
        LockCache<String> lockCache
    ) {
      return new FilePipelineStoreTask(runtimeInfo, stageLibraryTask, pipelineStateStore, lockCache);
    }

    @Provides
    @Singleton
    public FileAclStoreTask provideAclStoreTask(
        RuntimeInfo runtimeInfo,
        PipelineStoreTask pipelineStoreTask,
        LockCache<String> lockCache,
        UserGroupManager userGroupManager
    ) {
      return new FileAclStoreTask(runtimeInfo, pipelineStoreTask, lockCache, userGroupManager);
    }
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(new TestFileAclStoreTask.Module());
    PipelineStoreTask filePipelineStoreTask = dagger.get(PipelineStoreTask.class);
    store = new CachePipelineStoreTask(filePipelineStoreTask, new LockCache<String>());

    FileAclStoreTask fileAclStoreTask = dagger.get(FileAclStoreTask.class);
    UserGroupManager userGroupManager = dagger.get(UserGroupManager.class);
    aclStore = new CacheAclStoreTask(fileAclStoreTask, filePipelineStoreTask, new LockCache<String>(), userGroupManager);

  }

  @After
  public void tearDown() {
    store = null;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSaveAndFetchAcl() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Acl acl = aclStore.createAcl(
          TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
          ResourceType.PIPELINE,
          System.currentTimeMillis(),
          "testUser"
      );

      Acl fetchedAcl = aclStore.getAcl(TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME);
      Assert.assertNotNull(fetchedAcl);
      Assert.assertEquals(acl.getResourceId(), fetchedAcl.getResourceId());
      Assert.assertEquals(acl.getPermissions().size(), fetchedAcl.getPermissions().size());
      Assert.assertEquals("testUser", acl.getPermissions().get(0).getSubjectId());

      Permission newUserPermission = new Permission();
      newUserPermission.setSubjectId("user1");
      newUserPermission.setSubjectType(SubjectType.USER);
      newUserPermission.setLastModifiedBy("testUser");
      newUserPermission.setLastModifiedOn(System.currentTimeMillis());
      newUserPermission.setActions(ImmutableList.of(Action.READ));
      fetchedAcl.getPermissions().add(newUserPermission);

      aclStore.saveAcl(TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME, fetchedAcl);

      fetchedAcl = aclStore.getAcl(TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME);
      Assert.assertNotNull(fetchedAcl);
      Assert.assertEquals(2, fetchedAcl.getPermissions().size());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testUpdateSubjectsInAcl() throws Exception {
    store.init();
    List<String> users = new ArrayList<>();
    int numberOfPipelines = 10, numberOfUsers = 7, numberOfUsersInPipeline = 4;
    Map<String, String> oldUsersToNewUsers = new HashMap<>();
    for (int u = 0; u < numberOfUsers; u++) {
      String userName = "user" + u;
      String newUserName = "newUser" + u;
      users.add(userName);
      //Some users will not have mapping
      if (u < 5) {
        oldUsersToNewUsers.put(userName, newUserName);
      }
    }

    for (int p = 0; p < numberOfPipelines; p++) {
      String pipelineName = TestCachePipelineStoreTask.DEFAULT_PIPELINE_NAME + p;
      //To randomly select some users for the pipeline acl
      Collections.shuffle(users);
      List<String> usersWithAclForThisPipeline = users.subList(0, numberOfUsersInPipeline);
      String owner = users.get(0);
      createPipeline(store, pipelineName, owner);
      usersWithAclForThisPipeline = usersWithAclForThisPipeline.subList(1, usersWithAclForThisPipeline.size());
      Acl acl = aclStore.createAcl(
          pipelineName,
          ResourceType.PIPELINE,
          System.currentTimeMillis(),
          owner
      );
      for (String userWithAclForThisPipeline : usersWithAclForThisPipeline) {
        Permission permission = new Permission();
        permission.setLastModifiedBy(owner);
        permission.setSubjectId(userWithAclForThisPipeline);
        permission.setSubjectType(SubjectType.USER);
        permission.setActions(ImmutableList.of(Action.READ, Action.WRITE));
        permission.setLastModifiedOn(System.currentTimeMillis());
        acl.getPermissions().add(permission);
      }
      aclStore.saveAcl(pipelineName, acl);
    }

    // a pipeline without acl.
    createDefaultPipeline(store);


    aclStore.updateSubjectsInAcls(oldUsersToNewUsers);

    for (PipelineInfo pipelineInfo : store.getPipelines()) {
      Acl acl = aclStore.getAcl(pipelineInfo.getPipelineId());
      //Pipeline with no acl
      if (pipelineInfo.getPipelineId().equals(TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME)) {
        // Updated code to create ACL for pipelines with no ACL to help upgraded pipelines
        Assert.assertNotNull(acl);
      }
      String owner = acl.getResourceOwner();
      Assert.assertEquals(
          (oldUsersToNewUsers.containsKey(owner)) ? oldUsersToNewUsers.get(owner) : owner,
          owner
      );
      for (Permission permission : acl.getPermissions()) {
        Assert.assertEquals(
            (oldUsersToNewUsers.containsKey(owner)) ? oldUsersToNewUsers.get(owner) : owner,
            permission.getLastModifiedBy()
        );
        String user = permission.getSubjectId();
        Assert.assertEquals(
            (oldUsersToNewUsers.containsKey(user)) ? oldUsersToNewUsers.get(user) : user,
            user
        );
      }
    }
  }

  private void createPipeline(PipelineStoreTask store, String pipelineName, String user) throws PipelineException {
    store.create(
        user,
        pipelineName,
        "label",
        TestCachePipelineStoreTask.DEFAULT_PIPELINE_DESCRIPTION,
        false,
        false
    );
  }

  private void createDefaultPipeline(PipelineStoreTask store) throws PipelineException {
    store.create(
        TestFilePipelineStoreTask.SYSTEM_USER,
        TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
        "label",
        TestFilePipelineStoreTask.DEFAULT_PIPELINE_DESCRIPTION,
        false,
        false
    );
  }
}
