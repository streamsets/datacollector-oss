/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.validation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.configupgrade.PipelineConfigurationUpgrader;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.TextUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class TestPipelineConfigurationValidator {

  @Test
  public void testValidConfiguration() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());
  }

  @Test
  public void testRequiredInactiveConfig() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineWithRequiredDependentConfig();
    StageConfiguration stageConf = conf.getStages().get(0);
    stageConf.setConfig(
        Lists.newArrayList(new Config("dependencyConfName", 0),
                           new Config("triggeredConfName", null)));

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());

    stageConf.setConfig(
        Lists.newArrayList(new Config("dependencyConfName", 1),
                           new Config("triggeredConfName", null)));

    validator = new PipelineConfigurationValidator(lib, "name", conf);
      Assert.assertTrue(validator.validate().getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());
    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());
  }

  @Test
  public void testSpaceInName() {
    Assert.assertTrue(TextUtils.isValidName("Hello World"));
  }

  @Test
  public void testInvalidSchemaVersion() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget(0);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertTrue(validator.validate().getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());
    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getIssues().getPipelineIssues().get(0).getMessage().contains("VALIDATION_0000"));
  }

  @Test
  public void testExecutionModes() {
    StageLibraryTask lib = MockStages.createStageLibrary();

    // cluster only stage can not preview/run as standalone
    PipelineConfiguration conf = MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.STANDALONE);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertTrue(validator.validate().getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());
    Assert.assertTrue(validator.getIssues().hasIssues());

    // cluster only stage can preview  and run as cluster
    conf = MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER);
    validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
  }

  @Test
  public void testUpgradeIssues() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    conf.setVersion(conf.getVersion() + 1); //a version we don't handle

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);

    Assert.assertTrue(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.getIssues().hasIssues());
  }

  @Test
  public void testUpgradeOK() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();

    // tweak validator upgrader to require upgrading the pipeline
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator = Mockito.spy(validator);
    PipelineConfigurationUpgrader upgrader = Mockito.spy(new PipelineConfigurationUpgrader(){});
    int currentVersion = upgrader.getPipelineDefinition().getVersion();
    StageDefinition pipelineDef = Mockito.spy(upgrader.getPipelineDefinition());
    Mockito.when(pipelineDef.getVersion()).thenReturn(currentVersion + 1);
    Mockito.when(pipelineDef.getUpgrader()).thenReturn(new StageUpgrader() {
      @Override
      public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion,
                                  int toVersion,
                                  List<Config> configs) throws StageException {
        return configs;
      }
    });
    Mockito.when(upgrader.getPipelineDefinition()).thenReturn(pipelineDef);
    Mockito.when(validator.getUpgrader()).thenReturn(upgrader);

    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertEquals(currentVersion + 1, conf.getVersion());;
  }

  @Test
  public void testLibraryAlias() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    String stageLib = conf.getStages().get(0).getLibrary();

    conf.getStages().get(0).setLibrary("foo");

    lib = Mockito.spy(lib);
    Mockito.when(lib.getLibraryNameAliases()).thenReturn(ImmutableMap.of("foo", stageLib));

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertFalse(conf.getIssues().hasIssues());
    Assert.assertEquals(stageLib, conf.getStages().get(0).getLibrary());
  }

  @Test
  public void testEmptyValueRequiredField() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfTargetWithReqField();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertTrue(conf.getIssues().hasIssues());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0007.name(), issues.get(0).getErrorCode());
  }

  @Test
  public void testInvalidRequiredFieldsName() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceTargetWithRequiredFields();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertTrue(conf.getIssues().hasIssues());
    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0033.name(), issues.get(0).getErrorCode());
  }

  @Test
  public void testAddMissingConfigs() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    int pipelineConfigs = conf.getConfiguration().size();
    int stageConfigs = conf.getStages().get(2).getConfiguration().size();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());

    Assert.assertTrue(pipelineConfigs < conf.getConfiguration().size());
    Assert.assertTrue(stageConfigs < conf.getStages().get(2).getConfiguration().size());
  }

}
