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
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStore;
import com.streamsets.pipeline.store.PipelineStoreException;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import java.util.*;

public class TestFilePipelineStore {

  @dagger.Module(injects = FilePipelineStore.class)
  public static class Module {
    private boolean createDefaultPipeline;

    public Module(boolean createDefaultPipeline) {
      this.createDefaultPipeline = createDefaultPipeline;
    }

    @Provides
    public Configuration provideConfiguration() {
      Configuration conf = new Configuration();
      conf.set(FilePipelineStore.CREATE_DEFAULT_PIPELINE_KEY, createDefaultPipeline);
      return conf;
    }

    @Provides
    @Singleton
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getDataDir()).thenReturn("target/" + UUID.randomUUID());
      return mock;
    }
  }

  @Test
  public void testStoreNoDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      //creating store dir
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.destroy();
    }
    store = dagger.get(FilePipelineStore.class);
    try {
      //store dir already exists
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testStoreDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      //creating store dir and default pipeline
      store.init();
      Assert.assertEquals(1, store.getPipelines().size());
      Assert.assertEquals(1, store.getHistory(FilePipelineStore.DEFAULT_PIPELINE_NAME).size());
    } finally {
      store.destroy();
    }
    store = dagger.get(FilePipelineStore.class);
    try {
      //store dir exists and default pipeline already exists
      store.init();
      Assert.assertEquals(1, store.getPipelines().size());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testStoreDefaultPipelineInfo() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      List<PipelineInfo> infos = store.getPipelines();
      Assert.assertEquals(1, infos.size());
      PipelineInfo info = infos.get(0);
      Assert.assertNotNull(info.getUuid());
      Assert.assertEquals(FilePipelineStore.DEFAULT_PIPELINE_NAME, info.getName());
      Assert.assertEquals(FilePipelineStore.DEFAULT_PIPELINE_DESCRIPTION, info.getDescription());
      Assert.assertEquals(FilePipelineStore.SYSTEM_USER, info.getCreator());
      Assert.assertEquals(FilePipelineStore.SYSTEM_USER, info.getLastModifier());
      Assert.assertNotNull(info.getCreated());
      Assert.assertEquals(info.getLastModified(), info.getCreated());
      Assert.assertEquals(FilePipelineStore.REV, info.getLastRev());
      Assert.assertFalse(info.isValid());
      PipelineConfiguration pc = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      Assert.assertEquals(info.getUuid(), pc.getUuid());
      Assert.assertTrue(pc.getStages().isEmpty());
    } finally {
      store.destroy();
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
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

  @Test(expected = PipelineStoreException.class)
  public void testCreateExistingPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    PipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      store.create("a", "A", "foo");
      store.create("a", "A", "foo");
    } finally {
      store.destroy();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testDeleteNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      store.delete("a");
    } finally {
      store.destroy();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    FilePipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      store.save("a", "foo", null, null, pc);
    } finally {
      store.destroy();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveWrongUuid() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    FilePipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStore.DEFAULT_PIPELINE_NAME, FilePipelineStore.REV);
      pc.setUuid(UUID.randomUUID());
      store.save(FilePipelineStore.DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
    } finally {
      store.destroy();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testLoadNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      store.load("a", null);
    } finally {
      store.destroy();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testHistoryNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStore store = dagger.get(FilePipelineStore.class);
    try {
      store.init();
      store.getHistory("a");
    } finally {
      store.destroy();
    }
  }

  private PipelineConfiguration createPipeline(UUID uuid) {
    ConfigConfiguration config = new ConfigConfiguration("a", "B");
    Map<String, Object> uiInfo = new LinkedHashMap<String, Object>();
    uiInfo.put("ui", "UI");
    StageConfiguration stage = new StageConfiguration(
      "instance", "instance", "description", "library", "name", "version",
      ImmutableList.of(config), uiInfo, null, ImmutableList.of("a"));
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(3);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.ATLEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    return new PipelineConfiguration(uuid, "This is the pipeline description", pipelineConfigs, ImmutableList.of(stage));
  }

  @Test
  public void testSave() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
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
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
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
      Assert.assertEquals(pc.getUuid(), pc2.getUuid());
      PipelineInfo info = store.getInfo(FilePipelineStore.DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(pc.getUuid(), info.getUuid());
    } finally {
      store.destroy();
    }
  }

}
