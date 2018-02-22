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
package com.streamsets.datacollector.validation;

import com.google.common.base.Joiner;
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

import java.util.LinkedList;
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
  public void testEmptyTitle() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTarget();
    conf.setTitle("");
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertTrue(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0093.name(), issues.get(0).getErrorCode());
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
    conf = MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER_BATCH);
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
    StageConfiguration stageConf = conf.getStages().get(0);
    String stageLib = stageConf.getLibrary();
    String stageName = stageConf.getStageName();
    stageConf.setLibrary("fooLib");
    stageConf.setStageName("fooStage");
    lib = Mockito.spy(lib);
    Mockito.when(lib.getLibraryNameAliases()).thenReturn(ImmutableMap.of("fooLib", stageLib));
    Mockito.when(lib.getStageNameAliases()).thenReturn(ImmutableMap.of(Joiner.on(",").join(stageLib, "fooStage"),
      Joiner.on(",").join(stageLib, stageName)));
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertFalse(String.valueOf(conf.getIssues().getIssues()), conf.getIssues().hasIssues());
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
  public void testEmptyValueRequiredMapField() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfTargetWithRequiredMapField();
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

    // Generate error stage and clean up it's configuration
    StageConfiguration errorStage = MockStages.getErrorStageConfig();
    errorStage.setConfig(new LinkedList<Config>());
    conf.setErrorStage(errorStage);

    int pipelineConfigs = conf.getConfiguration().size();
    int stageConfigs = conf.getStages().get(2).getConfiguration().size();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());

    Assert.assertTrue(pipelineConfigs < conf.getConfiguration().size());
    Assert.assertTrue(stageConfigs < conf.getStages().get(2).getConfiguration().size());

    // Verify that error stage configs were generated as expected
    Assert.assertTrue(conf.getErrorStage().getConfiguration().size() > 0);
  }

  @Test
  public void testValidatePipelineConfigs() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf =  MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER_MESOS_STREAMING);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertTrue(conf.getIssues().hasIssues());
    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(2, issues.size());
    //mesosDispatcherURL is required but not set
    Assert.assertEquals(ValidationError.VALIDATION_0007.name(), issues.get(0).getErrorCode());
    //hdfsS3ConfDir is required but not set
    Assert.assertEquals(ValidationError.VALIDATION_0007.name(), issues.get(1).getErrorCode());
  }

  @Test
  public void testValidateOffsetControlMultipleTargets() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineWith2OffsetCommitController(ExecutionMode.STANDALONE);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertTrue(conf.getIssues().hasIssues());
    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0091.name(), issues.get(0).getErrorCode());
  }

  @Test
  public void testValidateOffsetControlDeliveryGuarantee() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineWithOffsetCommitController(ExecutionMode.STANDALONE);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    conf = validator.validate();
    Assert.assertTrue(conf.getIssues().hasIssues());
    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0092.name(), issues.get(0).getErrorCode());
  }

  // Having event lane empty when source is declaring events is acceptable
  @Test
  public void testPipelineWithOpenEventLane() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceTargetWithEventsOpen();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
  }

  // Stages should be re-ordered such that event stages are called when all it's input are properly processed
  @Test
  public void testPipelineWithConnectedEventLane() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceTargetWithEventsProcessedUnsorted();
    // The pipeline should declare the event target as first
    Assert.assertEquals("e", conf.getStages().get(0).getInstanceName());
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());

    List<StageConfiguration> stages = conf.getStages();
    Assert.assertNotNull(stages);
    Assert.assertEquals(3, stages.size());
    // Source should be ordered first (even though that the pipeline declares the event target as first)
    Assert.assertEquals("s", stages.get(0).getInstanceName());

    // Verify that event stages are marked accordingly
    Assert.assertFalse(stages.get(0).isInEventPath());
    Assert.assertFalse(stages.get(1).isInEventPath());
    Assert.assertTrue(stages.get(2).isInEventPath());
  }

  // Stage having event lane without declaring support for event is an error
  @Test
  public void testPipelineDeclaredEventLaneWithoutSupportingEvents() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceTargetDeclaredEventLaneWithoutSupportingEvents();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(2, issues.size());
    // Event lane declared on stage that doesn't produce events
    Assert.assertEquals(ValidationError.VALIDATION_0102.name(), issues.get(0).getErrorCode());
    // The definition contains open lane "e" that is not connected anywhere
    Assert.assertEquals(ValidationError.VALIDATION_0104.name(), issues.get(1).getErrorCode());
  }

  // Stage having event lane without declaring support for event is an error
  @Test
  public void testPipelineMergingEventAndDataLanes() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceProcessorTargetWithMergingEventAndDataLane();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(1, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0103.name(), issues.get(0).getErrorCode());
  }

  // Proper pipeline lifecycle event configuration
  @Test
  public void testPipelineLifecycleEvents() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationLifecycleEvents();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
  }

  // Incorrect configuration - input and event lanes
  @Test
  public void testPipelineLifecycleEventsIncorrect() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationLifecycleEventsIncorrect();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(2, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0036.name(), issues.get(0).getErrorCode());
    Assert.assertEquals(ValidationError.VALIDATION_0012.name(), issues.get(1).getErrorCode());
  }

  // Incorrect configuration - cluster mode
  @Test
  public void testPipelineLifecycleEventsClusterMode() {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfiguration conf = MockStages.createPipelineConfigurationLifecycleEvents();
    conf.getConfiguration().remove(conf.getConfiguration("executionMode"));
    conf.getConfiguration().add(new Config("executionMode", ExecutionMode.CLUSTER_BATCH));

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    validator.validate();

    Assert.assertTrue(validator.getIssues().hasIssues());
    Assert.assertFalse(validator.canPreview());

    List<Issue> issues = conf.getIssues().getIssues();
    Assert.assertEquals(2, issues.size());
    Assert.assertEquals(ValidationError.VALIDATION_0106.name(), issues.get(0).getErrorCode());
    Assert.assertEquals(ValidationError.VALIDATION_0106.name(), issues.get(1).getErrorCode());
  }

  @Test
  public void testFragmentUnrolled() {
    doTestFragmentUnrolled(MockStages.createPipelineConfigSourceFragmentTarget());
  }

  @Test
  public void testNestedFragmentUnrolled() {
    doTestFragmentUnrolled(MockStages.createPipelineConfigSourceFragmentInsideFragmentTarget());
  }

  private void doTestFragmentUnrolled(PipelineConfiguration conf) {
    StageLibraryTask lib = MockStages.createStageLibrary();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(lib, "name", conf);
    Assert.assertFalse(validator.validate().getIssues().hasIssues());
    Assert.assertTrue(validator.canPreview());
    Assert.assertFalse(validator.getIssues().hasIssues());
    Assert.assertTrue(validator.getOpenLanes().isEmpty());
  }
}
