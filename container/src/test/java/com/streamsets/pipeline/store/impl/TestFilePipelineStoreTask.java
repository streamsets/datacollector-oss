/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.MemoryLimitExceeded;
import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
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
import org.junit.Assert;
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

public class TestFilePipelineStoreTask {

  private static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String DEFAULT_PIPELINE_DESCRIPTION = "Default Pipeline";
  private static final String SYSTEM_USER = "system";

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

  }

  @Test
  public void testStoreNoDefaultPipeline() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module());
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
  public void testCreateDelete() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module());
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      store.save("a", "foo", null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testSaveWrongUuid() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc.setUuid(UUID.randomUUID());
      store.save(DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
    } finally {
      store.stop();
    }
  }

  @Test(expected = PipelineStoreException.class)
  public void testLoadNotExisting() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module());
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    FilePipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineInfo info1 = store.getInfo(DEFAULT_PIPELINE_NAME);
      PipelineConfiguration pc0 = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      pc0 = createPipeline(pc0.getUuid());
      Thread.sleep(5);
      store.save(DEFAULT_PIPELINE_NAME, "foo", null, null, pc0);
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineConfiguration pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Assert.assertTrue(pc.getStages().isEmpty());
      UUID uuid = pc.getUuid();
      pc = createPipeline(pc.getUuid());
      pc = store.save(DEFAULT_PIPELINE_NAME, "foo", null, null, pc);
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    store.create(DEFAULT_PIPELINE_NAME,DEFAULT_PIPELINE_DESCRIPTION, SYSTEM_USER);
  }

  @Test
  public void testPipelineDefaults() throws Exception {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    try {
      store.init();
      PipelineConfiguration pc = store.create(UUID.randomUUID().toString(),"", "foo");
      Map<String, Object> confs = new HashMap<>();
      for (ConfigConfiguration cc : pc.getConfiguration()) {
        confs.put(cc.getName(), cc.getValue());
      }
      Assert.assertEquals(DeliveryGuarantee.AT_LEAST_ONCE.name(), confs.get(PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG));
      Assert.assertEquals(ExecutionMode.STANDALONE.name(), confs.get(PipelineDefConfigs.EXECUTION_MODE_CONFIG));
      Assert.assertEquals("", confs.get(PipelineDefConfigs.ERROR_RECORDS_CONFIG));
      Assert.assertEquals(PipelineDefConfigs.MEMORY_LIMIT_DEFAULT, confs.get(PipelineDefConfigs.MEMORY_LIMIT_CONFIG));
      Assert.assertEquals(MemoryLimitExceeded.STOP_PIPELINE.name(), confs.get(PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG));
      Assert.assertEquals(Integer.parseInt(PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_DEFAULT),
                          confs.get(PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_CONFIG));
      Assert.assertEquals(false, confs.get(PipelineDefConfigs.CLUSTER_KERBEROS_AUTH_CONFIG));
      Assert.assertEquals("", confs.get(PipelineDefConfigs.CLUSTER_KERBEROS_PRINCIPAL_CONFIG));
      Assert.assertEquals("", confs.get(PipelineDefConfigs.CLUSTER_KERBEROS_KEYTAB_CONFIG));
      Assert.assertEquals(Collections.emptyList(), confs.get(PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_CONFIG));
    } finally {
      store.stop();
    }
  }

  @Test
  public void testSyncPipelineConfigOptions() throws PipelineStoreException {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    store.init();

    List<ConfigConfiguration> pipelineConfig = new ArrayList<>();
    pipelineConfig.add(new ConfigConfiguration("invalidPipelineConfigOption", "invalidPipelineConfigValue"));

    PipelineConfiguration expectedPipelineConfig = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
      UUID.randomUUID(), null, pipelineConfig, null, Collections.<StageConfiguration>emptyList(), null);

    /*
     * BEFORE SYNC -
     * Pipeline has 1 unexpected pipeline level config option and missing the 8 expected valid configs
     */
    Assert.assertTrue(expectedPipelineConfig.getConfiguration().size() == 1);
    Assert.assertEquals("invalidPipelineConfigOption", expectedPipelineConfig.getConfiguration().get(0).getName());

    /*
     * SYNC
     */
    expectedPipelineConfig = ((FilePipelineStoreTask)store).syncPipelineConfiguration(expectedPipelineConfig,
      "myPipeline", "1.0", MockStages.createStageLibrary());

    /*
     * AFTER SYNC -
     * Pipeline has 12 configurations with default values, the unexpected config option is removed
     */

    Assert.assertEquals(12, expectedPipelineConfig.getConfiguration().size());

    Assert.assertEquals(PipelineDefConfigs.EXECUTION_MODE_CONFIG,
      expectedPipelineConfig.getConfiguration().get(0).getName());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(),
      expectedPipelineConfig.getConfiguration().get(0).getValue());

    Assert.assertEquals(PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG,
      expectedPipelineConfig.getConfiguration().get(1).getName());
    Assert.assertEquals(DeliveryGuarantee.AT_LEAST_ONCE.name(),
      expectedPipelineConfig.getConfiguration().get(1).getValue());

    Assert.assertEquals(PipelineDefConfigs.ERROR_RECORDS_CONFIG,
      expectedPipelineConfig.getConfiguration().get(2).getName());
    Assert.assertEquals("", expectedPipelineConfig.getConfiguration().get(2).getValue());

    Assert.assertEquals(PipelineDefConfigs.CONSTANTS_CONFIG,
      expectedPipelineConfig.getConfiguration().get(3).getName());
    Assert.assertEquals(null, expectedPipelineConfig.getConfiguration().get(3).getValue());

    Assert.assertEquals(PipelineDefConfigs.MEMORY_LIMIT_CONFIG,
      expectedPipelineConfig.getConfiguration().get(4).getName());
    Assert.assertEquals(PipelineDefConfigs.MEMORY_LIMIT_DEFAULT,
      expectedPipelineConfig.getConfiguration().get(4).getValue());

    Assert.assertEquals(PipelineDefConfigs.MEMORY_LIMIT_EXCEEDED_CONFIG,
      expectedPipelineConfig.getConfiguration().get(5).getName());
    Assert.assertEquals(MemoryLimitExceeded.STOP_PIPELINE.name(),
      expectedPipelineConfig.getConfiguration().get(5).getValue());

    Assert.assertEquals(PipelineDefConfigs.CLUSTER_SLAVE_MEMORY_CONFIG,
      expectedPipelineConfig.getConfiguration().get(6).getName());
    Assert.assertEquals(1024, expectedPipelineConfig.getConfiguration().get(6).getValue());


    Assert.assertEquals(PipelineDefConfigs.CLUSTER_SLAVE_JAVA_OPTS_CONFIG,
      expectedPipelineConfig.getConfiguration().get(7).getName());
    Assert.assertEquals(PipelineDefConfigs.CLUSTER_SLAVE_JAVA_OPTS_DEFAULT,
      expectedPipelineConfig.getConfiguration().get(7).getValue());

    Assert.assertEquals(PipelineDefConfigs.CLUSTER_KERBEROS_AUTH_CONFIG,
      expectedPipelineConfig.getConfiguration().get(8).getName());
    Assert.assertEquals("false", expectedPipelineConfig.getConfiguration().get(8).getValue());

    Assert.assertEquals(PipelineDefConfigs.CLUSTER_KERBEROS_PRINCIPAL_CONFIG,
      expectedPipelineConfig.getConfiguration().get(9).getName());
    Assert.assertEquals("", expectedPipelineConfig.getConfiguration().get(9).getValue());

    Assert.assertEquals(PipelineDefConfigs.CLUSTER_KERBEROS_KEYTAB_CONFIG,
      expectedPipelineConfig.getConfiguration().get(10).getName());
    Assert.assertEquals("", expectedPipelineConfig.getConfiguration().get(10).getValue());

    Assert.assertEquals(PipelineDefConfigs.CLUSTER_LAUNCHER_ENV_CONFIG,
      expectedPipelineConfig.getConfiguration().get(11).getName());
    Assert.assertEquals("", expectedPipelineConfig.getConfiguration().get(11).getValue());
  }

  @Test
  public void testSyncPipelineConfiguration() throws PipelineStoreException {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    expectedPipelineConfig = ((FilePipelineStoreTask)store).syncPipelineConfiguration(expectedPipelineConfig, "myPipeline", "1.0",
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    expectedPipelineConfig = ((FilePipelineStoreTask)store).syncPipelineConfiguration(expectedPipelineConfig, "myPipeline", "1.0",
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

    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
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
    expectedPipelineConfig = ((FilePipelineStoreTask)store).syncPipelineConfiguration(expectedPipelineConfig,
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
    ObjectGraph dagger = ObjectGraph.create(new Module());
    PipelineStoreTask store = dagger.get(FilePipelineStoreTask.class);
    store.init();

    PipelineConfiguration expectedPipelineConfig = MockStages.createPipelineWithRequiredDependentConfig();

    StageConfiguration nonExistingStage = new StageConfiguration("nonExistingStage", "default", "nonExistingStage", "1.0.0",
      Collections.<ConfigConfiguration>emptyList(), null, ImmutableList.of("p"), Collections.<String>emptyList());
    expectedPipelineConfig.getStages().add(nonExistingStage);

    try {
      ((FilePipelineStoreTask)store).syncPipelineConfiguration(expectedPipelineConfig, "p", "1",
        MockStages.createStageLibrary());
    } catch (PipelineStoreException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0207, e.getErrorCode());
    }

  }

}
