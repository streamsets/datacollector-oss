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
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.store.FilePipelineStateStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import dagger.ObjectGraph;
import dagger.Provides;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  protected ObjectGraph dagger;
  protected static File samplePipelinesDir;

  @dagger.Module(
      injects = {
          Configuration.class,
          RuntimeInfo.class,
          FilePipelineStoreTask.class,
          LockCache.class,
          EventListenerManager.class,
          PipelineCredentialHandler.class
      },
      includes = LockCacheModule.class
  )
  public static class Module {
    public Module() {
    }

    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      return new Configuration();
    }
    @Provides
    @Singleton
    public BuildInfo provideBuildInfo() {
      return ProductBuildInfo.getDefault();
    }

    @Provides
    @Singleton
    public RuntimeInfo provideRuntimeInfo() {
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getDataDir()).thenReturn("target/" + UUID.randomUUID());
      Mockito.when(mock.getSamplePipelinesDir()).thenReturn(samplePipelinesDir.getAbsolutePath());
      return mock;
    }


    @Provides
    @Singleton
    public EventListenerManager provideEventListenerManager() {
      return Mockito.spy(new EventListenerManager());
    }

    @Provides
    @Singleton
    public StageLibraryTask provideStageLibrary() {
      return MockStages.createStageLibrary();
    }

    @Provides
    @Singleton
    public PipelineStateStore providePipelineStateStore(RuntimeInfo runtimeInfo) {
      return Mockito.spy(new FilePipelineStateStore(runtimeInfo, new Configuration()));
    }

    @Provides
    @Singleton
    public PipelineCredentialHandler provideEncryptingCredentialsHandler() {
      return Mockito.mock(PipelineCredentialHandler.class);
    }

    @Provides
    @Singleton
    public FilePipelineStoreTask providePipelineStoreTask(
        BuildInfo buildInfo,
        Configuration configuration,
        RuntimeInfo runtimeInfo,
        StageLibraryTask stageLibraryTask,
        PipelineStateStore pipelineStateStore,
        EventListenerManager eventListenerManager,
        LockCache<String> lockCache,
        PipelineCredentialHandler encryptingCredentialsHandler
    ) {
      return new FilePipelineStoreTask(
          buildInfo,
          runtimeInfo,
          stageLibraryTask,
          pipelineStateStore,
          eventListenerManager,
          lockCache,
          encryptingCredentialsHandler,
          configuration
      );
    }
  }

  @Before
  public void setUp() throws IOException {
    samplePipelinesDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(samplePipelinesDir.mkdirs());
    String samplePipeline = new File(samplePipelinesDir, "helloWorldPipeline.json").getAbsolutePath();
    OutputStream os = new FileOutputStream(samplePipeline);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("helloWorldPipeline.json"), os);

    dagger = ObjectGraph.create(new Module());
    store = dagger.get(FilePipelineStoreTask.class);
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
      store.create("foo", "a","label", "A", false, false, new HashMap<String, Object>());
      Assert.assertEquals(1, store.getPipelines().size());
      store.save("foo2", "a", FilePipelineStoreTask.REV, "A", store.load("a", "0"), false);
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
      store.create("foo", "a", "label", "A", false, false, new HashMap<String, Object>());
      store.create("foo", "a", "label", "A", false, false, new HashMap<String, Object>());
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
      store.save("foo", "a", null, null, pc, false);
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
      store.save("foo", DEFAULT_PIPELINE_NAME, null, null, pc, false);
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
      store.save("foo", DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, null, pc0, false);
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
  public void testSaveWithEncryptCredentials() throws Exception {
    try {
      store.init();
      PipelineCredentialHandler encryptingCredentialsHandler = dagger.get(PipelineCredentialHandler.class);
      Mockito.doNothing().when(encryptingCredentialsHandler)
          .handlePipelineConfigCredentials(Mockito.any(PipelineConfiguration.class));
      createDefaultPipeline(store);
      PipelineConfiguration pc0 = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
      Thread.sleep(5);
      store.save("foo", DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, null, pc0, true);
      Mockito.verify(encryptingCredentialsHandler).handlePipelineConfigCredentials(Mockito.eq(pc0));
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
      pc = store.save("foo", DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, null, pc, false);
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
    store.create(SYSTEM_USER, DEFAULT_PIPELINE_NAME, "label", DEFAULT_PIPELINE_DESCRIPTION, false, false,
        new HashMap<String, Object>()
    );
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
      pc = store.save("foo", DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, null, pc, false);

      // verify uiInfo stays after save
      Assert.assertEquals(2, pc.getUiInfo().size());
      Assert.assertEquals("A", pc.getUiInfo().get("a"));
      Assert.assertEquals("B", pc.getUiInfo().get("b"));
      Assert.assertEquals(2, pc.getStages().get(0).getUiInfo().size());
      Assert.assertEquals("IA", pc.getStages().get(0).getUiInfo().get("ia"));
      Assert.assertEquals("IB", pc.getStages().get(0).getUiInfo().get("ib"));

      // load and verify uiInfo stays
      pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
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
      store.saveUiInfo(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, uiInfo);

      pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
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

      store.saveMetadata(SYSTEM_USER, DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV, metadata);

      pc = store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);
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

  @Test
  public void testDeleteBroadcastState() throws Exception {
    EventListenerManager eventListenerManager = dagger.get(EventListenerManager.class);
    try {
      store.init();
      String id = UUID.randomUUID().toString();
      store.create(
          SYSTEM_USER,
          id,
          "label",
          DEFAULT_PIPELINE_DESCRIPTION,
          false,
          false,
          new HashMap<>()
      );

      ArgumentCaptor<PipelineState> fromStateArgumentCaptor = ArgumentCaptor.forClass(PipelineState.class);
      ArgumentCaptor<PipelineState> toStateArgumentCaptor = ArgumentCaptor.forClass(PipelineState.class);

      store.delete(id);
      Mockito.verify(eventListenerManager).broadcastStateChange(fromStateArgumentCaptor.capture(), toStateArgumentCaptor.capture(), Mockito.any(), Mockito.anyMap());
      PipelineState fromState = fromStateArgumentCaptor.getValue();
      PipelineState toState = toStateArgumentCaptor.getValue();
      Assert.assertEquals(id, fromState.getPipelineId());
      Assert.assertEquals(id, toState.getPipelineId());
      Assert.assertEquals(PipelineStatus.EDITED, fromState.getStatus());
      Assert.assertEquals(PipelineStatus.DELETED, toState.getStatus());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDeleteAndGetInfo() throws PipelineException, IOException {
    try {
      store.init();
      createDefaultPipeline(store);
      PipelineInfo infoBeforeDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      Path pipelineDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      String pipelineDirPath = pipelineDir.toAbsolutePath().toString();
      File infoFile = new File(pipelineDirPath.endsWith("/")
                               ? pipelineDirPath + DEFAULT_PIPELINE_NAME + "/info.json"
                               : pipelineDirPath + "/" + DEFAULT_PIPELINE_NAME + "/info.json");
      Assert.assertTrue(infoFile.exists());
      Files.delete(infoFile.toPath());

      PipelineInfo infoAfterDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      Assert.assertEquals(infoBeforeDelete.getPipelineId(), infoAfterDelete.getPipelineId());
      Assert.assertEquals(infoBeforeDelete.getTitle(), infoAfterDelete.getTitle());
      Assert.assertEquals(infoBeforeDelete.getDescription(), infoAfterDelete.getDescription());
      Assert.assertEquals(infoBeforeDelete.getCreated(), infoAfterDelete.getCreated());
      Assert.assertEquals(infoBeforeDelete.getLastModified(), infoAfterDelete.getLastModified());
      Assert.assertEquals(infoBeforeDelete.getCreator(), infoAfterDelete.getCreator());
      Assert.assertEquals(infoBeforeDelete.getLastModifier(), infoAfterDelete.getLastModifier());
      Assert.assertEquals(infoBeforeDelete.getLastRev(), infoAfterDelete.getLastRev());
      Assert.assertEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(infoBeforeDelete.isValid(), infoAfterDelete.isValid());
      Assert.assertEquals(infoBeforeDelete.getMetadata(), infoAfterDelete.getMetadata());
      Assert.assertEquals(infoBeforeDelete.getSdcVersion(), infoAfterDelete.getSdcVersion());
      Assert.assertEquals(infoBeforeDelete.getSdcId(), infoAfterDelete.getSdcId());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDeleteSaveAndGetInfo() throws PipelineException, IOException {
    try {
      store.init();
      PipelineConfiguration pipelineConfig = store.create(
          SYSTEM_USER,
          DEFAULT_PIPELINE_NAME,
          "label",
          DEFAULT_PIPELINE_DESCRIPTION,
          false,
          false,
          new HashMap<String, Object>());
      PipelineInfo infoBeforeDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      Path pipelineDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      String pipelineDirPath = pipelineDir.toAbsolutePath().toString();
      File infoFile = new File(pipelineDirPath.endsWith("/")
                               ? pipelineDirPath + DEFAULT_PIPELINE_NAME + "/info.json"
                               : pipelineDirPath + "/" + DEFAULT_PIPELINE_NAME + "/info.json");
      Assert.assertTrue(infoFile.exists());
      Files.delete(infoFile.toPath());

      PipelineConfiguration pipelineConfigurationAfterSave = store.save(
          "otherUser",
          DEFAULT_PIPELINE_NAME,
          FilePipelineStoreTask.REV,
          null,
          pipelineConfig,
          false
      );

      PipelineInfo infoAfterDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      Assert.assertEquals(infoBeforeDelete.getPipelineId(), infoAfterDelete.getPipelineId());
      Assert.assertEquals(infoBeforeDelete.getTitle(), infoAfterDelete.getTitle());
      Assert.assertEquals(infoBeforeDelete.getDescription(), infoAfterDelete.getDescription());
      Assert.assertEquals(infoBeforeDelete.getCreated(), infoAfterDelete.getCreated());
      Assert.assertNotEquals(infoBeforeDelete.getLastModified(), infoAfterDelete.getLastModified());
      Assert.assertEquals(infoBeforeDelete.getCreator(), infoAfterDelete.getCreator());
      Assert.assertEquals(SYSTEM_USER, infoBeforeDelete.getLastModifier());
      Assert.assertEquals("otherUser", infoAfterDelete.getLastModifier());
      Assert.assertEquals(infoBeforeDelete.getLastRev(), infoAfterDelete.getLastRev());
      Assert.assertNotEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(pipelineConfig.getUuid(), pipelineConfigurationAfterSave.getUuid());
      Assert.assertEquals(pipelineConfigurationAfterSave.getUuid(), infoAfterDelete.getUuid());
      Assert.assertNotEquals(infoBeforeDelete.isValid(), infoAfterDelete.isValid());
      Assert.assertEquals(infoBeforeDelete.getMetadata(), infoAfterDelete.getMetadata());
      Assert.assertEquals(infoBeforeDelete.getSdcVersion(), infoAfterDelete.getSdcVersion());
      Assert.assertEquals(infoBeforeDelete.getSdcId(), infoAfterDelete.getSdcId());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDeleteLoadAndGetInfo() throws PipelineException, IOException {
    try {
      store.init();
      PipelineConfiguration pipelineConfig = store.create(
          SYSTEM_USER,
          DEFAULT_PIPELINE_NAME,
          "label",
          DEFAULT_PIPELINE_DESCRIPTION,
          false,
          false,
          new HashMap<String, Object>());
      PipelineInfo infoBeforeDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      Path pipelineDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      String pipelineDirPath = pipelineDir.toAbsolutePath().toString();
      File infoFile = new File(pipelineDirPath.endsWith("/")
                               ? pipelineDirPath + DEFAULT_PIPELINE_NAME + "/info.json"
                               : pipelineDirPath + "/" + DEFAULT_PIPELINE_NAME + "/info.json");
      Assert.assertTrue(infoFile.exists());
      Files.delete(infoFile.toPath());

      store.load(DEFAULT_PIPELINE_NAME, FilePipelineStoreTask.REV);

      PipelineInfo infoAfterDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      Assert.assertEquals(infoBeforeDelete.getPipelineId(), infoAfterDelete.getPipelineId());
      Assert.assertEquals(infoBeforeDelete.getTitle(), infoAfterDelete.getTitle());
      Assert.assertEquals(infoBeforeDelete.getDescription(), infoAfterDelete.getDescription());
      Assert.assertEquals(infoBeforeDelete.getCreated(), infoAfterDelete.getCreated());
      Assert.assertEquals(infoBeforeDelete.getLastModified(), infoAfterDelete.getLastModified());
      Assert.assertEquals(infoBeforeDelete.getCreator(), infoAfterDelete.getCreator());
      Assert.assertEquals(infoBeforeDelete.getLastModifier(), infoAfterDelete.getLastModifier());
      Assert.assertEquals(infoBeforeDelete.getLastRev(), infoAfterDelete.getLastRev());
      Assert.assertEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(infoBeforeDelete.isValid(), infoAfterDelete.isValid());
      Assert.assertEquals(infoBeforeDelete.getMetadata(), infoAfterDelete.getMetadata());
      Assert.assertEquals(infoBeforeDelete.getSdcVersion(), infoAfterDelete.getSdcVersion());
      Assert.assertEquals(infoBeforeDelete.getSdcId(), infoAfterDelete.getSdcId());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDeleteGetPipelinesAndGetInfo() throws PipelineException, IOException {
    try {
      store.init();
      PipelineConfiguration pipelineConfig = store.create(
          SYSTEM_USER,
          DEFAULT_PIPELINE_NAME,
          "label",
          DEFAULT_PIPELINE_DESCRIPTION,
          false,
          false,
          new HashMap<String, Object>());
      PipelineInfo infoBeforeDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      Path pipelineDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      String pipelineDirPath = pipelineDir.toAbsolutePath().toString();
      File infoFile = new File(pipelineDirPath.endsWith("/")
                               ? pipelineDirPath + DEFAULT_PIPELINE_NAME + "/info.json"
                               : pipelineDirPath + "/" + DEFAULT_PIPELINE_NAME + "/info.json");
      Assert.assertTrue(infoFile.exists());
      Files.delete(infoFile.toPath());

      List<PipelineInfo> pipelineInfoList = store.getPipelines();
      Assert.assertEquals(1, pipelineInfoList.size());
      PipelineInfo infoAfterDelete = pipelineInfoList.get(0);

      Assert.assertEquals(infoBeforeDelete.getPipelineId(), infoAfterDelete.getPipelineId());
      Assert.assertEquals(infoBeforeDelete.getTitle(), infoAfterDelete.getTitle());
      Assert.assertEquals(infoBeforeDelete.getDescription(), infoAfterDelete.getDescription());
      Assert.assertEquals(infoBeforeDelete.getCreated(), infoAfterDelete.getCreated());
      Assert.assertEquals(infoBeforeDelete.getLastModified(), infoAfterDelete.getLastModified());
      Assert.assertEquals(infoBeforeDelete.getCreator(), infoAfterDelete.getCreator());
      Assert.assertEquals(infoBeforeDelete.getLastModifier(), infoAfterDelete.getLastModifier());
      Assert.assertEquals(infoBeforeDelete.getLastRev(), infoAfterDelete.getLastRev());
      Assert.assertEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(infoBeforeDelete.isValid(), infoAfterDelete.isValid());
      Assert.assertEquals(infoBeforeDelete.getMetadata(), infoAfterDelete.getMetadata());
      Assert.assertEquals(infoBeforeDelete.getSdcVersion(), infoAfterDelete.getSdcVersion());
      Assert.assertEquals(infoBeforeDelete.getSdcId(), infoAfterDelete.getSdcId());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testCreateDeleteSaveMetadataAndGetInfo() throws PipelineException, IOException {
    try {
      store.init();
      PipelineConfiguration pipelineConfig = store.create(
          SYSTEM_USER,
          DEFAULT_PIPELINE_NAME,
          "label",
          DEFAULT_PIPELINE_DESCRIPTION,
          false,
          false,
          new HashMap<String, Object>());
      PipelineInfo infoBeforeDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      Path pipelineDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      String pipelineDirPath = pipelineDir.toAbsolutePath().toString();
      File infoFile = new File(pipelineDirPath.endsWith("/")
                               ? pipelineDirPath + DEFAULT_PIPELINE_NAME + "/info.json"
                               : pipelineDirPath + "/" + DEFAULT_PIPELINE_NAME + "/info.json");
      Assert.assertTrue(infoFile.exists());
      Files.delete(infoFile.toPath());

      PipelineConfiguration pipelineConfigAfterSave = store.saveMetadata(
          "otherUser",
          DEFAULT_PIPELINE_NAME,
          FilePipelineStoreTask.REV,
          Collections.emptyMap()
      );

      PipelineInfo infoAfterDelete = store.getInfo(DEFAULT_PIPELINE_NAME);

      Assert.assertEquals(infoBeforeDelete.getPipelineId(), infoAfterDelete.getPipelineId());
      Assert.assertEquals(infoBeforeDelete.getTitle(), infoAfterDelete.getTitle());
      Assert.assertEquals(infoBeforeDelete.getDescription(), infoAfterDelete.getDescription());
      Assert.assertEquals(infoBeforeDelete.getCreated(), infoAfterDelete.getCreated());
      Assert.assertNotEquals(infoBeforeDelete.getLastModified(), infoAfterDelete.getLastModified());
      Assert.assertEquals(infoBeforeDelete.getCreator(), infoAfterDelete.getCreator());
      Assert.assertEquals(SYSTEM_USER, infoBeforeDelete.getLastModifier());
      Assert.assertEquals("otherUser", infoAfterDelete.getLastModifier());
      Assert.assertEquals(infoBeforeDelete.getLastRev(), infoAfterDelete.getLastRev());
      Assert.assertEquals(infoBeforeDelete.getUuid(), infoAfterDelete.getUuid());
      Assert.assertEquals(infoBeforeDelete.isValid(), infoAfterDelete.isValid());
      Assert.assertNotEquals(infoBeforeDelete.getMetadata(), infoAfterDelete.getMetadata());
      Assert.assertNull(infoBeforeDelete.getMetadata());
      Assert.assertEquals(0, infoAfterDelete.getMetadata().size());
      Assert.assertEquals(infoBeforeDelete.getSdcVersion(), infoAfterDelete.getSdcVersion());
      Assert.assertEquals(infoBeforeDelete.getSdcId(), infoAfterDelete.getSdcId());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testSamplePipelines() throws PipelineException {
    try {
      store.init();
      List<PipelineInfo> samplePipelines = store.getSamplePipelines();
      Assert.assertEquals(1, samplePipelines.size());
      Assert.assertEquals("helloWorldPipeline", samplePipelines.get(0).getPipelineId());

      PipelineEnvelopeJson pipelineEnvelope = store.loadSamplePipeline("helloWorldPipeline");
      Assert.assertNotNull(pipelineEnvelope);
      Assert.assertNotNull(pipelineEnvelope.getPipelineConfig());
      Assert.assertNotNull(pipelineEnvelope.getPipelineRules());

      // test loading invalid sample pipeline Id
      try {
        store.loadSamplePipeline("invalidPipeline");
        Assert.fail("Excepted exception");
      } catch (PipelineStoreException ex) {
        Assert.assertEquals("CONTAINER_0215", ex.getErrorCode().getCode());
      }

    } finally {
      store.stop();
    }
  }


}
