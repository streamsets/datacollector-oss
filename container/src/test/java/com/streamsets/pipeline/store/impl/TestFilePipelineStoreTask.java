/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestFilePipelineStoreTask {

  @dagger.Module(injects = FilePipelineStoreTask.class)
  public static class Module {
    private boolean createDefaultPipeline;

    public Module(boolean createDefaultPipeline) {
      this.createDefaultPipeline = createDefaultPipeline;
    }

    @Provides
    public Configuration provideConfiguration() {
      Configuration conf = new Configuration();
      conf.set(FilePipelineStoreTask.CREATE_DEFAULT_PIPELINE_KEY, createDefaultPipeline);
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
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      //creating store dir
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.stop();
    }
    store = dagger.get(FilePipelineStoreTask.class);
    try {
      //store dir already exists
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testStoreDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      //creating store dir and default pipeline
      store.init();
      Assert.assertEquals(1, store.getPipelines().size());
      Assert.assertEquals(1, store.getHistory(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME).size());
    } finally {
      store.stop();
    }
    store = dagger.get(FilePipelineStoreTask.class);
    try {
      //store dir exists and default pipeline already exists
      store.init();
      Assert.assertEquals(1, store.getPipelines().size());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testStoreDefaultPipelineInfo() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      List<PipelineInfo> infos = store.getPipelines();
      Assert.assertEquals(1, infos.size());
      PipelineInfo info = infos.get(0);
      Assert.assertNotNull(info.getUuid());
      Assert.assertEquals(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, info.getName());
      Assert.assertEquals(FilePipelineStoreTask.DEFAULT_PIPELINE_DESCRIPTION, info.getDescription());
      Assert.assertEquals(FilePipelineStoreTask.SYSTEM_USER, info.getCreator());
      Assert.assertEquals(FilePipelineStoreTask.SYSTEM_USER, info.getLastModifier());
      Assert.assertNotNull(info.getCreated());
      Assert.assertEquals(info.getLastModified(), info.getCreated());
      Assert.assertEquals(FilePipelineStoreTask.REV, info.getLastRev());
      Assert.assertFalse(info.isValid());
      PipelineConfiguration pc = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertEquals(info.getUuid(), pc.getUuid());
      Assert.assertTrue(pc.getStages().isEmpty());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      Assert.assertEquals(0, store.getPipelines().size());
      store.create("a", "A", "foo");
      Assert.assertEquals(1, store.getPipelines().size());
      Assert.assertEquals("a", store.getInfo("a").getName());
      store.delete("a");
      Assert.assertEquals(0, store.getPipelines().size());
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testCreateExistingPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      store.create("a", "A", "foo");
      store.create("a", "A", "foo");
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testDeleteNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      store.delete("a");
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      store.save("a", "foo", null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveWrongUuid() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc.setUuid(UUID.randomUUID());
      store.save(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testLoadNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      store.load("a", null);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testHistoryNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(false));
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      store.getHistory("a");
    } finally {
      store.stop();
    }
  }

  private PipelineConfiguration createPipeline(UUID uuid) {
    ConfigConfiguration config = new ConfigConfiguration("a", "B");
    Map<String, Object> uiInfo = new LinkedHashMap<String, Object>();
    uiInfo.put("ui", "UI");
    StageConfiguration stage = new StageConfiguration(
      "instance", "library", "name", "version",
      ImmutableList.of(config), uiInfo, null, ImmutableList.of("a"));
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<ConfigConfiguration>(3);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    return new PipelineConfiguration(uuid, pipelineConfigs, null, ImmutableList.of(stage));
  }

  @Test
  public void testSave() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      PipelineInfo info1 = store.getInfo(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME);
      PipelineConfiguration pc0 = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc0 = createPipeline(pc0.getUuid());
      Thread.sleep(5);
      store.save(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, "foo", null, null, pc0);
      PipelineInfo info2 = store.getInfo(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(info1.getCreated(), info2.getCreated());
      Assert.assertEquals(info1.getCreator(), info2.getCreator());
      Assert.assertEquals(info1.getName(), info2.getName());
      Assert.assertEquals(info1.getLastRev(), info2.getLastRev());
      Assert.assertEquals("foo", info2.getLastModifier());
      Assert.assertTrue(info2.getLastModified().getTime() > info1.getLastModified().getTime());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testSaveAndLoad() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      PipelineConfiguration pc = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertTrue(pc.getStages().isEmpty());
      UUID uuid = pc.getUuid();
      pc = createPipeline(pc.getUuid());
      pc = store.save(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
      UUID newUuid = pc.getUuid();
      Assert.assertNotEquals(uuid, newUuid);
      PipelineConfiguration pc2 = store.load(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertFalse(pc2.getStages().isEmpty());
      Assert.assertEquals(pc.getUuid(), pc2.getUuid());
      PipelineInfo info = store.getInfo(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(pc.getUuid(), info.getUuid());
    } finally {
      store.stop();
    }
  }

}
