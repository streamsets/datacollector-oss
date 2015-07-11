/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.google.common.collect.ImmutableList;

import static org.junit.Assert.assertEquals;

import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import com.streamsets.pipeline.config.RuleDefinitions;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.ContainerError;

import dagger.ObjectGraph;
import dagger.Provides;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestFilePipelineStoreTask {

  private static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String DEFAULT_PIPELINE_DESCRIPTION = "Default Pipeline";
  private static final String SYSTEM_USER = "system";
  protected PipelineStoreTask store;

  @dagger.Module(injects = FilePipelineStoreTask.class)
  public static class Module {
    public Module() {
    }
    @Provides
    @Singleton
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getDataDir()).thenReturn("target/" + UUID.randomUUID());
      Mockito.when(mock.getExecutionMode()).thenReturn(RuntimeInfo.ExecutionMode.STANDALONE);
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
  }

  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    FilePipelineStoreTask filePipelineStoreTask = dagger.get(FilePipelineStoreTask.class);
    store = new CachePipelineStoreTask(filePipelineStoreTask);
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
      store.create("foo", "a", "A");
      Assert.assertEquals(1, store.getPipelines().size());
      store.save("foo2", "a", "A", "", store.load("a", "0"));
      assertEquals("foo2", store.getPipelines().get(0).getLastModifier());
      Assert.assertEquals("a", store.getInfo("a").getName());
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
      store.create("foo", "a", "A");
      store.create("foo", "a", "A");
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
    } finally {
      store.stop();
    }
  }

  @Test
  public void testStoreAndRetrieveRules() throws PipelineStoreException {
    store.init();
    createDefaultPipeline(store);
    RuleDefinitions ruleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    Assert.assertNotNull(ruleDefinitions);
    Assert.assertTrue(ruleDefinitions.getDataRuleDefinitions().isEmpty());
    Assert.assertTrue(ruleDefinitions.getMetricsRuleDefinitions().isEmpty());

    List<MetricsRuleDefinition> metricsRuleDefinitions = ruleDefinitions.getMetricsRuleDefinitions();
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true));

    List<DataRuleDefinition> dataRuleDefinitions = ruleDefinitions.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions);

    RuleDefinitions actualRuleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinitions == actualRuleDefinitions);
  }

  @Test
  public void testStoreMultipleCopies() throws PipelineStoreException {
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
    RuleDefinitions ruleDefinitions2 = new RuleDefinitions(tempRuleDef.getMetricsRuleDefinitions(),
      tempRuleDef.getDataRuleDefinitions(), tempRuleDef.getEmailIds(), tempRuleDef.getUuid());

    List<MetricsRuleDefinition> metricsRuleDefinitions = ruleDefinitions1.getMetricsRuleDefinitions();
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m1", "m1", "a", MetricType.COUNTER,
      MetricElement.COUNTER_COUNT, "p", false, true));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m2", "m2", "a", MetricType.TIMER,
      MetricElement.TIMER_M15_RATE, "p", false, true));
    metricsRuleDefinitions.add(new MetricsRuleDefinition("m3", "m3", "a", MetricType.HISTOGRAM,
      MetricElement.HISTOGRAM_MEAN, "p", false, true));

    List<DataRuleDefinition> dataRuleDefinitions = ruleDefinitions2.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    //store ruleDefinition1
    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions1);

    //attempt storing rule definition 2, should fail
    try {
      store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions2);
      Assert.fail("Expected PipelineStoreException as the rule definition being saved is not the latest copy.");
    } catch (PipelineStoreException e) {
      Assert.assertEquals(e.getErrorCode(), ContainerError.CONTAINER_0205);
    }

    //reload, modify and and then store
    ruleDefinitions2 = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);
    dataRuleDefinitions = ruleDefinitions2.getDataRuleDefinitions();
    dataRuleDefinitions.add(new DataRuleDefinition("a", "a", "a", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("b", "b", "b", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));
    dataRuleDefinitions.add(new DataRuleDefinition("c", "c", "c", 20, 300, "x", true, "c", ThresholdType.COUNT, "200",
      1000, true, false, true));

    store.storeRules(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, ruleDefinitions2);

    RuleDefinitions actualRuleDefinitions = store.retrieveRules(DEFAULT_PIPELINE_NAME,
      FilePipelineStoreTask.REV);

    Assert.assertTrue(ruleDefinitions2 == actualRuleDefinitions);
  }

  private void createDefaultPipeline(PipelineStoreTask store) throws PipelineStoreException {
    store.create(SYSTEM_USER, DEFAULT_PIPELINE_NAME, DEFAULT_PIPELINE_DESCRIPTION);
  }

  @Test
  public void testSyncPipelineConfiguration() throws PipelineStoreException {
    store.init();

    /*
     * BEFORE SYNC -
     * 1. error stage has an unexpected config and does not have expected config
     * 2. Source has 0 configuration, expected 2
     * 3. Target has 1 configuration, expected 0
     */
    PipelineConfiguration expectedPipelineConfig = MockStages.createPipelineWithRequiredDependentConfig();

    //error stage has an unexpected config and does not have expected config
    List<ConfigConfiguration> configuration = new ArrayList<>();
      configuration.add(new ConfigConfiguration("Hello", "World"));
    expectedPipelineConfig.getErrorStage().setConfig(configuration);
    Assert.assertEquals(1, expectedPipelineConfig.getErrorStage().getConfiguration().size());

    //Source has 0 configuration, expected 2
    Assert.assertTrue(expectedPipelineConfig.getStages().get(0).getConfiguration().size() == 0);
    //Target has 1 configuration, expected 0
    Assert.assertTrue(expectedPipelineConfig.getStages().get(1).getConfiguration().size() == 0);
    expectedPipelineConfig.getStages().get(1).getConfiguration().add(new ConfigConfiguration("unexpected", "conf"));
    Assert.assertTrue(expectedPipelineConfig.getStages().get(1).getConfiguration().size() == 1);

    /*
     * SYNC
     */
    expectedPipelineConfig = getFilePipelineStoreTask().syncPipelineConfiguration(expectedPipelineConfig, "myPipeline", "1.0",
      MockStages.createStageLibrary());

    /*
     * AFTER SYNC -
     * 1. pipeline has 8 configurations with default values
     * 2. error stage has expected config with expected default value
     * 3. Source has 2 expected config with default values
     * 4. Target has 0 configs
     */

    //pipeline config should have 12 configs
    Assert.assertEquals(12, expectedPipelineConfig.getConfiguration().size());

    //error stage has an expected config
    Assert.assertEquals(1, expectedPipelineConfig.getErrorStage().getConfiguration().size());
    Assert.assertEquals("errorTargetConfName", expectedPipelineConfig.getErrorStage().getConfiguration().get(0).getName());
    Assert.assertEquals("/SDC_HOME/errorDir", expectedPipelineConfig.getErrorStage().getConfiguration().get(0).getValue());

    //Source has configuration, expected 2
    List<ConfigConfiguration> sourceConfig = expectedPipelineConfig.getStages().get(0).getConfiguration();
    Assert.assertTrue(sourceConfig.size() == 2);

    Assert.assertEquals("dependencyConfName", sourceConfig.get(0).getName());
    Assert.assertEquals(5, sourceConfig.get(0).getValue());

    Assert.assertEquals("triggeredConfName", sourceConfig.get(1).getName());
    Assert.assertEquals(10, sourceConfig.get(1).getValue());

    //Target has 1 configuration, expected 0
    Assert.assertTrue(expectedPipelineConfig.getStages().get(1).getConfiguration().size() == 0);
  }

  @Test
  public void testSyncPipelineComplexConfiguration1() throws PipelineStoreException {
    store.init();

    //Scenario 1 - expected complex config but has none
    PipelineConfiguration expectedPipelineConfig = MockStages.createPipelineConfigurationComplexSourceTarget();

    /*
     * BEFORE SYNC -
     * Source has 0 config, expected 1 complex config
     */
    Assert.assertTrue(expectedPipelineConfig.getStages().get(0).getConfiguration().size() == 0);

    /*
     * SYNC
     */
    expectedPipelineConfig = getFilePipelineStoreTask().syncPipelineConfiguration(expectedPipelineConfig, "myPipeline", "1.0",
      MockStages.createStageLibrary());

    /*
     * AFTER SYNC -
     * 1. Source has 1 complex config
     */

    //Source has configuration, expected 1
    List<ConfigConfiguration> sourceConfig = expectedPipelineConfig.getStages().get(0).getConfiguration();
    Assert.assertEquals(1, sourceConfig.size());
    Assert.assertEquals("complexConfName", sourceConfig.get(0).getName());
    Assert.assertTrue(sourceConfig.get(0).getValue() instanceof List);
    List value = (List) sourceConfig.get(0).getValue();
    Assert.assertEquals(1, value.size());

    Assert.assertTrue(value.get(0) instanceof Map);
    Map<String, Object> m = (Map<String, Object>) value.get(0);
    Assert.assertEquals(1, m.entrySet().size());

    Assert.assertTrue(m.containsKey("regularConfName"));
    Assert.assertEquals(10, m.get("regularConfName"));

  }

  @Test
  public void testSyncPipelineComplexConfiguration2() throws PipelineStoreException {
    store.init();

    PipelineConfiguration expectedPipelineConfig = MockStages.createPipelineConfigurationComplexSourceTarget();

     /*
     * BEFORE SYNC -
     * Source has many unexpected config
     */
    Assert.assertTrue(expectedPipelineConfig.getStages().get(0).getConfiguration().size() == 0);
    Map<String, Object> map1 = new HashMap<>();
    map1.put("unexpectedConfig1", "Unexpected value1");
    map1.put("unexpectedConfig2", "Unexpected value2");
    Map<String, Object> map2 = new HashMap<>();
    map2.put("unexpectedConfig3", "Unexpected value3");
    map2.put("unexpectedConfig4", "Unexpected value4");
    List<Map<String, Object>> listOfMap = new ArrayList<>();
    listOfMap.add(map1);
    listOfMap.add(map2);
    ConfigConfiguration c = new ConfigConfiguration("complexConfName", listOfMap);
    expectedPipelineConfig.getStages().get(0).setConfig(Arrays.asList(c));
    Assert.assertTrue(expectedPipelineConfig.getStages().get(0).getConfiguration().size() == 1);

    /*
     * SYNC
     */
    expectedPipelineConfig = getFilePipelineStoreTask().syncPipelineConfiguration(expectedPipelineConfig,
      "myPipeline", "1.0",
      MockStages.createStageLibrary());

    /*
     * AFTER SYNC -
     * 1. Source has expected complex config
     */

    //Source has configuration, expected 1
    List<ConfigConfiguration> sourceConfig = expectedPipelineConfig.getStages().get(0).getConfiguration();
    Assert.assertEquals(1, sourceConfig.size());
    Assert.assertEquals("complexConfName", sourceConfig.get(0).getName());
    Assert.assertTrue(sourceConfig.get(0).getValue() instanceof List);
    List value = (List) sourceConfig.get(0).getValue();
    Assert.assertEquals(2, value.size());

    Assert.assertTrue(value.get(0) instanceof Map);
    Map<String, Object> m = (Map<String, Object>) value.get(0);
    Assert.assertEquals(1, m.entrySet().size());

    Assert.assertTrue(m.containsKey("regularConfName"));
    Assert.assertEquals(10, m.get("regularConfName"));

    Assert.assertTrue(value.get(1) instanceof Map);
    m = (Map<String, Object>) value.get(1);
    Assert.assertEquals(1, m.entrySet().size());

    Assert.assertTrue(m.containsKey("regularConfName"));
    Assert.assertEquals(10, m.get("regularConfName"));
  }

  @Test
  public void testSyncPipelineConfigurationInvalidStage() throws PipelineStoreException {
    store.init();

    PipelineConfiguration expectedPipelineConfig = MockStages.createPipelineWithRequiredDependentConfig();

    StageConfiguration nonExistingStage = new StageConfiguration("nonExistingStage", "default", "nonExistingStage", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    expectedPipelineConfig.getStages().add(nonExistingStage);

    try {
      getFilePipelineStoreTask().syncPipelineConfiguration(expectedPipelineConfig, "p", "1",
        MockStages.createStageLibrary());
    } catch (PipelineStoreException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0207, e.getErrorCode());
    }
  }

  private FilePipelineStoreTask getFilePipelineStoreTask() {
    FilePipelineStoreTask filePipelineStoreTask;
    if (store instanceof FilePipelineStoreTask) {
      filePipelineStoreTask = ((FilePipelineStoreTask) store);
    } else {
      filePipelineStoreTask = (FilePipelineStoreTask) ((CachePipelineStoreTask) store).getActualStore();
    }
    return filePipelineStoreTask;
  }

}
