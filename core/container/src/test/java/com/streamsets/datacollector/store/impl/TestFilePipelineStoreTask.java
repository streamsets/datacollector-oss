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


import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.PipelineException;
import dagger.ObjectGraph;
import dagger.Provides;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestFilePipelineStoreTask {

  protected static final String DEFAULT_PIPELINE_NAME = "xyz";
  protected static final String DEFAULT_PIPELINE_DESCRIPTION = "Default Pipeline";
  protected static final String SYSTEM_USER = "system";
  protected PipelineStoreTask store;

  @dagger.Module(injects = {FilePipelineStoreTask.class, LockCache.class},
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
    public FilePipelineStoreTask providePipelineStoreTask(
        RuntimeInfo runtimeInfo,
        StageLibraryTask stageLibraryTask,
        PipelineStateStore pipelineStateStore,
        LockCache<String> lockCache
    ) {
      return new FilePipelineStoreTask(runtimeInfo, stageLibraryTask, pipelineStateStore, lockCache);
    }
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    FilePipelineStoreTask filePipelineStoreTask = dagger.get(FilePipelineStoreTask.class);
    store = new CachePipelineStoreTask(filePipelineStoreTask, new LockCache<String>());
  }

  @After
  public void tearDown() {
    store = null;
  }

  @Test
  public void testStoreNoDefaultPipeline() throws Exception {
    try {
      //creating store dir
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.stop();
    }
    try {
      //store dir already exists
      store.init();
      Assert.assertTrue(store.getPipelines().isEmpty());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDelete() throws Exception {
    try {
      store.init();
      Assert.assertEquals(0, store.getPipelines().size());
      store.create("foo", "a","label", "A", false, false);
      Assert.assertEquals(1, store.getPipelines().size());
      store.save("foo2", "a", "A", "", store.load("a", "0"));
      assertEquals("foo2", store.getPipelines().get(0).getLastModifier());
      Assert.assertEquals("a", store.getInfo("a").getPipelineId());
      store.delete("a");
      Assert.assertEquals(0, store.getPipelines().size());
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testCreateExistingPipeline() throws Exception {
    try {
      store.init();
      store.create("foo", "a", "label", "A", false, false);
      store.create("foo", "a", "label", "A", false, false);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testDeleteNotExisting() throws Exception {
    try {
      store.init();
      store.delete("a");
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveNotExisting() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      store.save("foo", "a", null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveWrongUuid() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc.setUuid(UUID.randomUUID());
      store.save("foo", DEFAULT_PIPELINE_NAME, null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testLoadNotExisting() throws Exception {
    try {
      store.init();
      store.load("a", null);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testHistoryNotExisting() throws Exception {
    try {
      store.init();
      store.getHistory("a");
    } finally {
      store.stop();
    }
  }

  private PipelineConfiguration createPipeline(UUID uuid) {
    PipelineConfiguration pc = MockStages.createPipelineConfigurationSourceTarget();
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("a", "A");
    pc.setMetadata(metadata);
    pc.setUuid(uuid);
    return pc;
  }

  @Test
  public void testSave() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineInfo info1 = store.getInfo(DEFAULT_PIPELINE_NAME);
      PipelineConfiguration pc0 = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc0 = createPipeline(pc0.getUuid());
      Thread.sleep(5);
      store.save("foo", DEFAULT_PIPELINE_NAME, null, null, pc0);
      PipelineInfo info2 = store.getInfo(DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(info1.getCreated(), info2.getCreated());
      Assert.assertEquals(info1.getCreator(), info2.getCreator());
      Assert.assertEquals(info1.getPipelineId(), info2.getPipelineId());
      Assert.assertEquals(info1.getLastRev(), info2.getLastRev());
      Assert.assertEquals("foo", info2.getLastModifier());
      Assert.assertTrue(info2.getLastModified().getTime() > info1.getLastModified().getTime());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testSaveAndLoad() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertTrue(pc.getStages().isEmpty());
      UUID uuid = pc.getUuid();
      pc = createPipeline(pc.getUuid());
      pc = store.save("foo", DEFAULT_PIPELINE_NAME, null, null, pc);
      UUID newUuid = pc.getUuid();
      Assert.assertNotEquals(uuid, newUuid);
      PipelineConfiguration pc2 = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertFalse(pc2.getStages().isEmpty());
      Assert.assertEquals(pc.getUuid(), pc2.getUuid());
      PipelineInfo info = store.getInfo(DEFAULT_PIPELINE_NAME);
      Assert.assertEquals(pc.getUuid(), info.getUuid());
      Assert.assertNotNull(pc2.getMetadata());
      Assert.assertEquals(pc.getMetadata(), pc2.getMetadata());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testStoreAndRetrieveRules() throws PipelineException {
    store.init();
    createDefaultPipeline(store);
    RuleDefinitions ruleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    Assert.assertNotNull(ruleDefinitions);
    Assert.assertTrue(ruleDefinitions.getDataRuleDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinitions.getMetricsRuleDefinitions().isEmpty());

    long timestamp = System.currentTimeMillis();
    List<MetricsRuleDefinition> metricsRuleDefinitions = ruleDefinitions.getMetricsRuleDefinitions();
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true, timestamp));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true, timestamp));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true, timestamp));

    List<DataRuleDefinition> dataRuleDefinitions = ruleDefinitions.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));

    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions, false);

    RuleDefinitions actualRuleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinitions == actualRuleDefinitions);
  }

  @Test
  public void testStoreMultipleCopies() throws PipelineException {
    /*This test case mimicks a use case where 2 users connect to the same data collector instance
    * using different browsers and modify the same rule definition. The user who saves last runs into an exception.
    * The user is forced to reload, reapply changes and save*/
    store.init();
    createDefaultPipeline(store);
    RuleDefinitions ruleDefinitions1 = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    RuleDefinitions tempRuleDef = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    //Mimick two different clients [browsers] retrieving from the store
    RuleDefinitions ruleDefinitions2 = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        tempRuleDef.getMetricsRuleDefinitions(),
        tempRuleDef.getDataRuleDefinitions(),
        new ArrayList<DriftRuleDefinition>(),
        Collections.emptyList(),
        tempRuleDef.getUuid(),
        Collections.emptyList()
    );

    long timestamp = System.currentTimeMillis();
    List<MetricsRuleDefinition> metricsRuleDefinitions = ruleDefinitions1.getMetricsRuleDefinitions();
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true, timestamp));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true, timestamp));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true, timestamp));

    List<DataRuleDefinition> dataRuleDefinitions = ruleDefinitions2.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));

    //store ruleDefinition1
    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions1, false);

    //attempt storing rule definition 2, should fail
    try {
      store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions2, false);
      Assert.fail("Expected PipelineStoreException as the rule definition being saved is not the latest copy.");
    } catch (PipelineStoreException e) {
      Assert.assertEquals(e.getErrorCode(), ContainerError.CONTAINER_0205);
    }

    //reload, modify and and then store
    ruleDefinitions2 = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    dataRuleDefinitions = ruleDefinitions2.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true, timestamp));

    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions2, false);

    RuleDefinitions actualRuleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinitions2 == actualRuleDefinitions);
  }

  private void createDefaultPipeline(PipelineStoreTask store) throws PipelineException {
    store.create(SYSTEM_USER, DEFAULT_PIPELINE_NAME, "label", DEFAULT_PIPELINE_DESCRIPTION, false, false);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUiInfoSave() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertTrue(pc.getUiInfo().isEmpty());

      // add a stage to pipeline
      pc.getStages().add(new StageConfigurationBuilder("i", "n").build());

      // set some uiInfo at pipeline and stage level and dave
      pc.getUiInfo().put("a", "A");
      pc.getUiInfo().put("b", "B");
      pc.getStages().get(0).getUiInfo().put("ia", "IA");
      pc.getStages().get(0).getUiInfo().put("ib", "IB");
      pc = store.save("foo", DEFAULT_PIPELINE_NAME, null, null, pc);

      // verify uiInfo stays after save
      Assert.assertEquals(2, pc.getUiInfo().size());
      Assert.assertEquals("A", pc.getUiInfo().get("a"));
      Assert.assertEquals("B", pc.getUiInfo().get("b"));
      Assert.assertEquals(2, pc.getStages().get(0).getUiInfo().size());
      Assert.assertEquals("IA", pc.getStages().get(0).getUiInfo().get("ia"));
      Assert.assertEquals("IB", pc.getStages().get(0).getUiInfo().get("ib"));

      // load and verify uiInfo stays
      pc = store.load(DEFAULT_PIPELINE_NAME, null);
      Assert.assertEquals(2, pc.getUiInfo().size());
      Assert.assertEquals("A", pc.getUiInfo().get("a"));
      Assert.assertEquals("B", pc.getUiInfo().get("b"));
      Assert.assertEquals(2, pc.getStages().get(0).getUiInfo().size());
      Assert.assertEquals("IA", pc.getStages().get(0).getUiInfo().get("ia"));
      Assert.assertEquals("IB", pc.getStages().get(0).getUiInfo().get("ib"));

      // extract uiInfo, modify and save uiInfo only
      Map<String, Object> uiInfo = FilePipelineStoreTask.extractUiInfo(pc);
      ((Map)uiInfo.get(":pipeline:")).clear();
      ((Map)uiInfo.get(":pipeline:")).put("a", "AA");
      ((Map)uiInfo.get("i")).clear();
      ((Map)uiInfo.get("i")).put("ia", "IIAA");
      store.saveUiInfo(DEFAULT_PIPELINE_NAME, null, uiInfo);

      pc = store.load(DEFAULT_PIPELINE_NAME, null);
      Assert.assertNotNull(pc.getUiInfo());
      Assert.assertEquals(1, pc.getUiInfo().size());
      Assert.assertEquals("AA", pc.getUiInfo().get("a"));
      Assert.assertEquals(1, pc.getStages().get(0).getUiInfo().size());
      Assert.assertEquals("IIAA",pc.getStages().get(0).getUiInfo().get("ia"));

    } finally {
      store.stop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMetadataSave() throws Exception {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertTrue(pc.getUiInfo().isEmpty());

      // extract uiInfo, modify and save uiInfo only
      Map<String, Object> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");

      store.saveMetadata(SYSTEM_USER, DEFAULT_PIPELINE_NAME, null, metadata);

      pc = store.load(DEFAULT_PIPELINE_NAME, null);
      Assert.assertNotNull(pc.getMetadata());
      Assert.assertEquals(2, pc.getMetadata().size());
      Assert.assertEquals("value1", pc.getMetadata().get("key1"));

      PipelineInfo info = store.getInfo(DEFAULT_PIPELINE_NAME);
      Assert.assertNotNull(info.getMetadata());
      Assert.assertEquals(2, info.getMetadata().size());
      Assert.assertEquals("value1", info.getMetadata().get("key1"));
    } finally {
      store.stop();
    }
  }


}
