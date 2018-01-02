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
package com.streamsets.datacollector.configupgrade;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.definition.ServiceDefinitionExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor;
import com.streamsets.datacollector.runner.preview.StageConfigurationBuilder;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.base.BaseSource;

import com.streamsets.pipeline.api.service.Service;
import com.streamsets.pipeline.api.service.ServiceDef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipelineConfigurationUpgrader {
  private static int UPGRADE_CALLED;
  private static int SERVICE_UPGRADE_CALLED;

  @Before
  public void setUp() {
    UPGRADE_CALLED = 0;
    SERVICE_UPGRADE_CALLED = 0;
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class Source1 extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public static class Upgrader2 implements StageUpgrader {
    @Override
    public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
      UPGRADE_CALLED++;
      configs.add(new Config("a", "A"));
      context.registerService(Runnable.class, ImmutableList.of(new Config("a", "A")));
      return configs;
    }
  }

  @StageDef(version = 1, label = "L", upgrader = Upgrader2.class, onlineHelpRefUrl = "")
  public static class Source2 extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public static class ServiceUpgrader implements StageUpgrader {
    @Override
    public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
      SERVICE_UPGRADE_CALLED++;
      configs.add(new Config("b", "B"));
      return configs;
    }
  }

  @ServiceDef(
    provides = Runnable.class,
    upgrader = ServiceUpgrader.class,
    version = 1,
    label = "Test Service for Runnable"
  )
  public static class RunnableService implements Service, Runnable {
    @Override
    public List<ConfigIssue> init(Context context) {
      return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void run() {

    }
  }

  private static final StageLibraryDefinition LIBRARY_DEF =
      StageLibraryDefinitionExtractor.get().extract(TestPipelineConfigurationUpgrader.class.getClassLoader());

  private static final StageDefinition SOURCE1_DEF = StageDefinitionExtractor.get().extract(LIBRARY_DEF,
                                                                                            Source1.class, "");

  private static final StageDefinition SOURCE2_V1_DEF = StageDefinitionExtractor.get().extract(LIBRARY_DEF,
                                                                                               Source2.class, "");
  private static final StageDefinition SOURCE2_V2_DEF;

  private static final ServiceDefinition SERVICE_DEF = ServiceDefinitionExtractor.get().extract(LIBRARY_DEF, RunnableService.class);

  static {
    SOURCE2_V2_DEF = Mockito.spy(SOURCE2_V1_DEF);
    Mockito.when(SOURCE2_V2_DEF.getVersion()).thenReturn(2);
  }

  private StageLibraryTask getLibrary(StageDefinition def, ServiceDefinition ...services) {
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getStage(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(def);

    for(ServiceDefinition serviceDef : services) {
      Mockito.when(library.getServiceDefinition(serviceDef.getProvides(), false)).thenReturn(serviceDef);
    }
    return library;
  }

  @Test
  public void testNeedsUpgradeStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .build();

    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(null, SOURCE2_V1_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(null, SOURCE2_V2_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    stageConf = new StageConfigurationBuilder("i",SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V2_DEF.getVersion())
      .build();

    // null def
    Assert.assertFalse(up.needsUpgrade(null, null, stageConf, issues));
    Assert.assertFalse(issues.isEmpty());

    // invalid downgrade
    issues.clear();
    Assert.assertFalse(up.needsUpgrade(null, SOURCE2_V1_DEF, stageConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  public static class ForMockUpgrader extends PipelineConfigurationUpgrader {
  }

  ;

  public PipelineConfigurationUpgrader getPipelineV2Upgrader() {
    PipelineConfigurationUpgrader up = new ForMockUpgrader();
    StageDefinition pipelineDefV2 = Mockito.spy(up.getPipelineDefinition());
    Mockito.when(pipelineDefV2.getVersion()).thenReturn(PipelineConfigBean.VERSION + 1);
    Mockito.when(pipelineDefV2.getUpgrader()).thenReturn(new Upgrader2());
    up = Mockito.spy(up);
    Mockito.when(up.getPipelineDefinition()).thenReturn(pipelineDefV2);
    return up;
  }

  @Test
  public void testNeedsUpgradePipelineConfs() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(getPipelineV2Upgrader().needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    PipelineConfigurationUpgrader up0 = getPipelineV2Upgrader();
    StageDefinition pipelineDefV0 = Mockito.spy(up.getPipelineDefinition());
    Mockito.when(pipelineDefV0.getVersion()).thenReturn(0);
    Mockito.when(up0.getPipelineDefinition()).thenReturn(pipelineDefV0);

    Assert.assertFalse(up0.needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testNeedsUpgradePipelineErrorStage() throws Exception {
    PipelineConfigurationUpgrader up = new ForMockUpgrader();

    StageConfiguration stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V1_DEF.getVersion())
      .build();

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        stageConf,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V2_DEF.getVersion())
      .build();
    pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        Collections.emptyList(),
        stageConf,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testNeedsUpgradePipelineStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V1_DEF.getVersion())
      .build();

    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        ImmutableList.of(stageConf),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V2_DEF.getVersion())
      .build();
    pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        ImmutableList.of(stageConf),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());

  }

  @Test
  public void testUpgradeStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfigurationBuilder("i", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V1_DEF.getVersion())
      .build();
    List<Issue> issues = new ArrayList<>();

    Assert.assertTrue(up.needsUpgrade(null, SOURCE2_V2_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    stageConf = up.upgradeIfNeeded(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), stageConf, issues);
    Assert.assertNotNull(stageConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), stageConf.getStageVersion());
    Assert.assertEquals(1, UPGRADE_CALLED);

    // Validate services
    Assert.assertEquals(1, SERVICE_UPGRADE_CALLED);
    Assert.assertEquals(1, stageConf.getServices().size());
    ServiceConfiguration serviceConf = stageConf.getServices().get(0);
    Assert.assertEquals(Runnable.class, serviceConf.getService());
    Assert.assertEquals(1, serviceConf.getServiceVersion());
    Assert.assertEquals("A", serviceConf.getConfig("a").getValue());
    Assert.assertEquals("B", serviceConf.getConfig("b").getValue());
  }

  private PipelineConfiguration getPipelineToUpgrade() {
    StageConfiguration stageConf1 = new StageConfigurationBuilder("i1", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V2_DEF.getVersion())
      .build();

    StageConfiguration stageConf2 = new StageConfigurationBuilder("i2", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V1_DEF.getVersion())
      .build();

    StageConfiguration errorConf = new StageConfigurationBuilder("e", SOURCE2_V1_DEF.getName())
      .withLibrary(SOURCE2_V1_DEF.getLibrary())
      .withStageVersion(SOURCE2_V1_DEF.getVersion())
      .build();

    return new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        Collections.emptyList(),
        null,
        ImmutableList.of(stageConf1, stageConf2),
        errorConf,
        null,
        null,
        null
    );
  }

  @Test
  public void testUpgradePipeline() throws Exception {
    PipelineConfigurationUpgrader up2 = getPipelineV2Upgrader();

    PipelineConfiguration pipelineConf = getPipelineToUpgrade();

    List<Issue> issues = new ArrayList<>();

    Assert.assertTrue(up2.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    pipelineConf = up2.upgrade(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getErrorStage().getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(0).getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(1).getStageVersion());
    Assert.assertEquals(3, UPGRADE_CALLED);
    Assert.assertEquals(PipelineConfigBean.VERSION + 1, pipelineConf.getVersion());
    Assert.assertEquals("A", pipelineConf.getConfiguration("a").getValue());
    Assert.assertEquals(null, pipelineConf.getStages().get(0).getConfig("a"));
    Assert.assertEquals(1, pipelineConf.getStages().get(1).getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getStages().get(1).getConfig("a").getValue());
    Assert.assertEquals(1, pipelineConf.getErrorStage().getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getErrorStage().getConfig("a").getValue());
  }

  @Test
  public void testUpgradeIfNecessaryPipelineUpgrade() throws Exception {
    PipelineConfigurationUpgrader up2 = getPipelineV2Upgrader();

    PipelineConfiguration pipelineConf = getPipelineToUpgrade();

    List<Issue> issues = new ArrayList<>();

    pipelineConf = up2.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(PipelineStoreTask.SCHEMA_VERSION, pipelineConf.getSchemaVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getErrorStage().getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(0).getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(1).getStageVersion());
    Assert.assertEquals(3, UPGRADE_CALLED);
    Assert.assertEquals(PipelineConfigBean.VERSION + 1, pipelineConf.getVersion());
    Assert.assertEquals("A", pipelineConf.getConfiguration("a").getValue());
    Assert.assertEquals(null, pipelineConf.getStages().get(0).getConfig("a"));
    Assert.assertEquals(1, pipelineConf.getStages().get(1).getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getStages().get(1).getConfig("a").getValue());
    Assert.assertEquals(1, pipelineConf.getErrorStage().getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getErrorStage().getConfig("a").getValue());
  }

  @Test
  public void testUpgradeSchemaVersion1to2() throws Exception {
    PipelineConfiguration pipelineConf = getPipelineToUpgrade();
    PipelineConfigurationUpgrader up = getPipelineV2Upgrader();

    List<Issue> issues = new ArrayList<>();
    pipelineConf = up.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(PipelineStoreTask.SCHEMA_VERSION, pipelineConf.getSchemaVersion());
    Assert.assertEquals(2, pipelineConf.getStages().size());

    for(StageConfiguration stage : pipelineConf.getStages()) {
      Assert.assertNotNull(stage.getEventLanes());
      Assert.assertEquals(0, stage.getEventLanes().size());
    }

    Assert.assertNotNull(pipelineConf.getErrorStage().getEventLanes());
    Assert.assertEquals(0, pipelineConf.getErrorStage().getEventLanes().size());
  }

  /**
   * Nulls for new startEventStages and stopEventStages should be replaced with empty lists
   */
  @Test
  public void testUpgradeSchemaVersion3to4() throws Exception {
    PipelineConfiguration pipelineConf = getPipelineToUpgrade();
    PipelineConfigurationUpgrader up = getPipelineV2Upgrader();

    List<Issue> issues = new ArrayList<>();

    Assert.assertNull(pipelineConf.getStartEventStages());
    Assert.assertNull(pipelineConf.getStopEventStages());

    pipelineConf = up.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF, SERVICE_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(PipelineStoreTask.SCHEMA_VERSION, pipelineConf.getSchemaVersion());

    Assert.assertNotNull(pipelineConf.getStartEventStages());
    Assert.assertTrue(pipelineConf.getStartEventStages().isEmpty());

    Assert.assertNotNull(pipelineConf.getStopEventStages());
    Assert.assertTrue(pipelineConf.getStopEventStages().isEmpty());
  }

}
