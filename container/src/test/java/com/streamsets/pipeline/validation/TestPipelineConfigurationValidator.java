/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.impl.TextUtils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ContainerError;
import org.junit.Assert;
import org.junit.Test;

public class TestPipelineConfigurationValidator {

  @Test
  public void testValidConfiguration() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertTrue(validator.validate());
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
        Lists.newArrayList(new ConfigConfiguration("dependencyConfName", 0),
                           new ConfigConfiguration("triggeredConfName", null)));

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertTrue(validator.validate());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());

    stageConf.setConfig(
        Lists.newArrayList(new ConfigConfiguration("dependencyConfName", 1),
                           new ConfigConfiguration("triggeredConfName", null)));

    validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate());
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
    Assert.assertFalse(validator.validate());
    Assert.assertFalse(validator.canPreview());
    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getIssues().getPipelineIssues().get(0).getMessage().contains("VALIDATION_0000"));
  }

}
