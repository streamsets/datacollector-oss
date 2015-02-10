/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DataAlertDefinition;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
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
    Map<String, Object> uiInfo = new LinkedHashMap<>();
    uiInfo.put("ui", "UI");
    StageConfiguration stage = new StageConfiguration(
      "instance", "library", "name", "version",
      ImmutableList.of(config), uiInfo, null, ImmutableList.of("a"));
    List<ConfigConfiguration> pipelineConfigs = new ArrayList<>(3);
    pipelineConfigs.add(new ConfigConfiguration("deliveryGuarantee", DeliveryGuarantee.AT_LEAST_ONCE));
    pipelineConfigs.add(new ConfigConfiguration("stopPipelineOnError", false));

    return new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, uuid, pipelineConfigs,
                                     null, ImmutableList.of(stage));
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

  @Test
  public void testStoreAndRetrieveRules() throws PipelineStoreException {
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    store.init();
    RuleDefinition ruleDefinition = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    Assert.assertNotNull(ruleDefinition);
    Assert.assertTrue(ruleDefinition.getDataAlertDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinition.getMetricsAlertDefinitions().isEmpty());

    List<MetricsAlertDefinition> metricsAlertDefinitions = ruleDefinition.getMetricsAlertDefinitions();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true));

    List<DataAlertDefinition> dataAlertDefinitions = ruleDefinition.getDataAlertDefinitions();
    dataAlertDefinitions.add(new DataAlertDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    store.storeRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinition);

    RuleDefinition actualRuleDefinition = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinition == actualRuleDefinition);
  }

  @Test
  public void testStoreMultipleCopies() throws PipelineStoreException {
    /*This test case mimicks a use case where 2 users connect to the same data collector instance
    * using different browsers and modify the same rule definition. The user who saves last runs into an exception.
    * The user is forced to reload, reapply changes and save*/
    ObjectGraph dagger = ObjectGraph.create(new Module(true));
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    store.init();
    RuleDefinition ruleDefinition1 = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    RuleDefinition tempRuleDef = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    //Mimick two different clients [browsers] retrieving from the store
    RuleDefinition ruleDefinition2 = new RuleDefinition(tempRuleDef.getMetricsAlertDefinitions(),
      tempRuleDef.getDataAlertDefinitions(), tempRuleDef.getEmailIds(), tempRuleDef.getUuid());

    List<MetricsAlertDefinition> metricsAlertDefinitions = ruleDefinition1.getMetricsAlertDefinitions();
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true));
    metricsAlertDefinitions.add(new MetricsAlertDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true));

    List<DataAlertDefinition> dataAlertDefinitions = ruleDefinition2.getDataAlertDefinitions();
    dataAlertDefinitions.add(new DataAlertDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    //store ruleDefinition1
    store.storeRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinition1);

    //attempt storing rule definition 2, should fail
    try {
      store.storeRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinition2);
      Assert.fail("Expected PipelineStoreException as the rule definition being saved is not the latest copy.");
    } catch (PipelineStoreException e) {
      Assert.assertEquals(e.getErrorCode(), ContainerError.CONTAINER_0205);
    }

    //reload, modify and and then store
    ruleDefinition2 = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    dataAlertDefinitions = ruleDefinition2.getDataAlertDefinitions();
    dataAlertDefinitions.add(new DataAlertDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataAlertDefinitions.add(new DataAlertDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    store.storeRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinition2);

    RuleDefinition actualRuleDefinition = store.retrieveRules(FilePipelineStoreTask.DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinition2 == actualRuleDefinition);
  }

}
