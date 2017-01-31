/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
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

import javax.inject.Singleton;
import java.util.UUID;

public class TestFileAclStoreTask {
  protected PipelineStoreTask store;
  protected AclStoreTask aclStore;

  @dagger.Module(injects = {PipelineStoreTask.class, FileAclStoreTask.class, LockCache.class},
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
    public PipelineStateStore providePipelineStateStore() {
      return null;
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
        LockCache<String> lockCache
    ) {
      return new FileAclStoreTask(runtimeInfo, pipelineStoreTask, lockCache);
    }
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(new TestFileAclStoreTask.Module());
    PipelineStoreTask filePipelineStoreTask = dagger.get(PipelineStoreTask.class);
    store = new CachePipelineStoreTask(filePipelineStoreTask, new LockCache<String>());

    FileAclStoreTask fileAclStoreTask = dagger.get(FileAclStoreTask.class);
    aclStore = new CacheAclStoreTask(fileAclStoreTask, filePipelineStoreTask);

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
      newUserPermission.setResourceId(acl.getResourceId());
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

  private void createDefaultPipeline(PipelineStoreTask store) throws PipelineException {
    store.create(
        TestFilePipelineStoreTask.SYSTEM_USER,
        TestFilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
        "label",
        TestFilePipelineStoreTask.DEFAULT_PIPELINE_DESCRIPTION,
        false
    );
  }
}
