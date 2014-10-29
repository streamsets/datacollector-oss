/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.store.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStore;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestFilePipelineStore {

  @Module(injects = FilePipelineStore.class)
  public static class TModule {
    private boolean createDefaultPipeline;

    public TModule(boolean createDefaultPipeline) {
      this.createDefaultPipeline = createDefaultPipeline;
    }

    @Provides
    public Configuration provideConfiguration() {
      Configuration conf = new Configuration();
      conf.set(FilePipelineStore.CREATE_DEFAULT_PIPELINE_KEY, createDefaultPipeline);
      return conf;
    }

    @Provides
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getDataDir()).thenReturn("target/" + UUID.randomUUID());
      return mock;
    }
  }

  @Test
  public void testStoreNoDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new TModule(false));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testStoreDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new TModule(true));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      List<PipelineInfo> infos = store.getPipelines();
      Assert.assertEquals(1, infos.size());
      PipelineInfo info = infos.get(0);
      Assert.assertEquals(FilePipelineStore.DEFAULT_PIPELINE_NAME, info.getName());
      Assert.assertEquals(FilePipelineStore.DEFAULT_PIPELINE_DESCRIPTION, info.getDescription());
      Assert.assertEquals(FilePipelineStore.SYSTEM_USER, info.getCreator());
      Assert.assertEquals(FilePipelineStore.SYSTEM_USER, info.getLastModifier());
      Assert.assertNotNull(info.getCreated());
      Assert.assertEquals(info.getLastModified(), info.getCreated());
      Assert.assertEquals(FilePipelineStore.REV, info.getLastRev());
      PipelineConfiguration pc = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      Assert.assertTrue(pc.getStages().isEmpty());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new TModule(false));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      Assert.assertEquals(0, store.getPipelines().size());
      store.create("a", "A", "foo");
      Assert.assertEquals(1, store.getPipelines().size());
      Assert.assertEquals("a", store.getInfo("a").getName());
      store.delete("a");
      Assert.assertEquals(0, store.getPipelines().size());
    } finally {
      store.destroy();
    }
  }

  private PipelineConfiguration createPipeline(UUID uuid) {
    ConfigConfiguration config = new ConfigConfiguration("a", "B");
    Map<String, Object> uiInfo = new LinkedHashMap<String, Object>();
    uiInfo.put("ui", "UI");
    StageConfiguration stage = new StageConfiguration("instance", "library", "name", "version",
                                                      ImmutableList.of(config), uiInfo,
                                                      new ArrayList<String>(), ImmutableList.of("a"));
    return new PipelineConfiguration(uuid, ImmutableList.of(stage));
  }

  @Test
  public void testSave() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new TModule(true));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      PipelineInfo info1 = store.getInfo(FilePipelineStore.DEFAULT_PIPELINE_NAME);
      PipelineConfiguration pc0 = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      pc0 = createPipeline(pc0.getUuid());
      Thread.sleep(5);
      store.save(FilePipelineStore.DEFAULT_PIPELINE_NAME, "foo", null, null, pc0);
      PipelineInfo info2 = store.getInfo(FilePipelineStore.DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(info1.getCreated(), info2.getCreated());
      Assert.assertEquals(info1.getCreator(), info2.getCreator());
      Assert.assertEquals(info1.getName(), info2.getName());
      Assert.assertEquals(info1.getLastRev(), info2.getLastRev());
      Assert.assertEquals("foo", info2.getLastModifier());
      Assert.assertTrue(info2.getLastModified().getTime() > info1.getLastModified().getTime());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testSaveAndLoad() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new TModule(true));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      Assert.assertTrue(pc.getStages().isEmpty());
      UUID uuid = pc.getUuid();
      pc = createPipeline(pc.getUuid());
      pc = store.save(FilePipelineStore.DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
      UUID newUuid = pc.getUuid();
      Assert.assertNotEquals(uuid, newUuid);
      PipelineConfiguration pc2 = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      Assert.assertFalse(pc2.getStages().isEmpty());
    } finally {
      store.destroy();
    }
  }
}
