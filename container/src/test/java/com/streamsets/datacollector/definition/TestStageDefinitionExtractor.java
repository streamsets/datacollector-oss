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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.SparkClusterType;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.creation.StageConfigBean;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestStageDefinitionExtractor {

  public enum Group1 implements Label {
    G1;

    @Override
    public String getLabel() {
      return "g1";
    }
  }

  public static class Previewer  implements RawSourcePreviewer {
    @Override
    public InputStream preview(int maxLength) {
      return null;
    }

    @Override
    public String getMimeType() {
      return null;
    }

    @Override
    public void setMimeType(String mimeType) {

    }
  }

  @StageDef(
      version = 1,
      label = "L",
      description = "D",
      icon = "TargetIcon.svg",
      libJarsRegex = {ClusterModeConstants.AVRO_JAR_REGEX, ClusterModeConstants.AVRO_MAPRED_JAR_REGEX},
      onlineHelpRefUrl = "",
      services = @ServiceDependency(service = Runnable.class, configuration = {
        @ServiceConfiguration(name = "country", value = "Czech"),
        @ServiceConfiguration(name = "importance", value = "high"),
      })
  )
  public static class Source1 extends BaseSource {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String config1;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String config2;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public static class Source2Upgrader implements StageUpgrader {
    @Override
    public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
        List<Config> configs) throws StageException {
      return configs;
    }
  }

  public enum TwoOutputStreams implements Label {
    OS1, OS2;

    @Override
    public String getLabel() {
      return name();
    }
  }

  @StageDef(version = 2, label = "LL", description = "DD", icon = "TargetIcon.svg",
      execution = {ExecutionMode.STANDALONE, ExecutionMode.CLUSTER_BATCH}, outputStreams = TwoOutputStreams.class, recordsByRef = true,
      privateClassLoader = true, upgrader = Source2Upgrader.class,
      onlineHelpRefUrl = ""
  )
  @ConfigGroups(Group1.class)
  @RawSource(rawSourcePreviewer = Previewer.class)
  @HideConfigs(value = "config2", preconditions = true, onErrorRecord = true)
  public static class Source2 extends Source1 {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String config3;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "X",
        required = true
    )
    public String config4;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(
      version = 1,
      label = "L",
      outputStreams = StageDef.VariableOutputStreams.class,
      outputStreamsDrivenByConfig = "config1",
      onlineHelpRefUrl = "",
      upgraderDef = "upgrader/source1.yaml"
  )
  public static class Source3 extends Source1 {

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @HideConfigs(preconditions = true)
  public static class Target1 extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class Target2 extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @HideConfigs({"this.config.does.not.exists"})
  public static class HideNonExistingConfigTarget extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @HideConfigs(preconditions = true)
  public static class Executor1 extends BaseExecutor {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @ErrorStage
  public static class ToErrorTarget1 extends Target1 {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "", icon="missing.svg", onlineHelpRefUrl = "")
  @ErrorStage
  public static class MissingIcon extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @PipelineLifecycleStage
  public static class PipelineLifecycleTarget extends Target1 {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", outputStreams = StageDef.VariableOutputStreams.class,
    outputStreamsDrivenByConfig = "config1", onlineHelpRefUrl = "")
  public static class OffsetCommitSource extends Source1 implements OffsetCommitTrigger {

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }

    @Override
    public boolean commit() {
      return false;
    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class OffsetCommitterPushSource extends BasePushSource implements OffsetCommitter {
    @Override
    public void commit(String offset) throws StageException {

    }

    @Override
    public int getNumberOfThreads() {
      return 0;
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "", producesEvents = true)
  public static class ProducesEventsTarger extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "Hidden stage", onlineHelpRefUrl = "")
  @HideStage(HideStage.Type.FIELD_PROCESSOR)
  public static class HiddenStage extends BaseProcessor {
    @Override
    public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    }
  }

  private static final StageLibraryDefinition MOCK_LIB_DEF =
      new StageLibraryDefinition(TestStageDefinitionExtractor.class.getClassLoader(), "mock", "MOCK", new Properties(),
                                 null, null, null);

  @Test
  public void testExtractSource1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Source1.class, "x");
    Assert.assertFalse(def.isPrivateClassLoader());
    Assert.assertEquals(Source1.class.getName(), def.getClassName());
    Assert.assertEquals(StageDefinitionExtractor.getStageName(Source1.class), def.getName());
    Assert.assertEquals(1, def.getVersion());
    Assert.assertEquals("L", def.getLabel());
    Assert.assertEquals("D", def.getDescription());
    Assert.assertEquals(null, def.getRawSourceDefinition());
    Assert.assertEquals(0, def.getConfigGroupDefinition().getGroupNames().size());
    Assert.assertEquals(3, def.getConfigDefinitions().size());
    Assert.assertEquals(1, def.getOutputStreams());
    Assert.assertEquals(5, def.getExecutionModes().size());
    Assert.assertEquals(2, def.getLibJarsRegex().size());
    Assert.assertEquals("TargetIcon.svg", def.getIcon());
    Assert.assertEquals(StageDef.DefaultOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertEquals(null, def.getOutputStreamLabels());
    Assert.assertEquals(StageType.SOURCE, def.getType());
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_REQUIRED_FIELDS_CONFIG));
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_PRECONDITIONS_CONFIG));
    Assert.assertTrue(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_ON_RECORD_ERROR_CONFIG));
    Assert.assertFalse(def.getRecordsByRef());
    Assert.assertTrue(def.getUpgrader() instanceof StageUpgrader.Default);
    Assert.assertFalse(def.isProducingEvents());

    Assert.assertEquals(1, def.getServices().size());
    ServiceDependencyDefinition service = def.getServices().get(0);
    Assert.assertNotNull(service);
    Assert.assertEquals(Runnable.class, service.getServiceClass());
    Assert.assertEquals(2, service.getConfiguration().size());
    Assert.assertTrue(service.getConfiguration().containsKey("country"));
    Assert.assertTrue(service.getConfiguration().containsKey("importance"));
    Assert.assertEquals("Czech", service.getConfiguration().get("country"));
    Assert.assertEquals("high", service.getConfiguration().get("importance"));

    Assert.assertNotNull(def.getHideStage());
    Assert.assertEquals(0, def.getHideStage().size());
  }

  @Test
  public void testExtractSource2() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Source2.class, "x");
    Assert.assertTrue(def.isPrivateClassLoader());
    Assert.assertEquals(Source2.class.getName(), def.getClassName());
    Assert.assertEquals(StageDefinitionExtractor.getStageName(Source2.class), def.getName());
    Assert.assertEquals(2, def.getVersion());
    Assert.assertEquals("LL", def.getLabel());
    Assert.assertEquals("DD", def.getDescription());
    Assert.assertNotNull(def.getRawSourceDefinition());
    Assert.assertEquals(1, def.getConfigGroupDefinition().getGroupNames().size());
    Assert.assertEquals(3, def.getConfigDefinitions().size());
    Assert.assertEquals(2, def.getOutputStreams());
    Assert.assertEquals(2, def.getExecutionModes().size());
    Assert.assertEquals("TargetIcon.svg", def.getIcon());
    Assert.assertEquals(TwoOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertEquals(null, def.getOutputStreamLabels());
    Assert.assertEquals(StageType.SOURCE, def.getType());
    Assert.assertFalse(def.isVariableOutputStreams());
    Assert.assertFalse(def.hasOnRecordError());
    Assert.assertFalse(def.hasPreconditions());
    Assert.assertTrue(def.getUpgrader() instanceof Source2Upgrader);
    Assert.assertFalse(def.isProducingEvents());
  }

  @Test
  public void testExtractSource2DisablePrivateCL() {
    String noPrivateCLProperty = Source2.class.getCanonicalName() + ".no.private.classloader";
    System.setProperty(noPrivateCLProperty, "");
    try {
      StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Source2.class, "x");
      Assert.assertFalse(def.isPrivateClassLoader());
      Assert.assertEquals(Source2.class.getName(), def.getClassName());
      Assert.assertEquals(StageDefinitionExtractor.getStageName(Source2.class), def.getName());
      Assert.assertEquals(2, def.getVersion());
      Assert.assertEquals("LL", def.getLabel());
      Assert.assertEquals("DD", def.getDescription());
      Assert.assertNotNull(def.getRawSourceDefinition());
      Assert.assertEquals(1, def.getConfigGroupDefinition().getGroupNames().size());
      Assert.assertEquals(3, def.getConfigDefinitions().size());
      Assert.assertEquals(2, def.getOutputStreams());
      Assert.assertEquals(2, def.getExecutionModes().size());
      Assert.assertEquals("TargetIcon.svg", def.getIcon());
      Assert.assertEquals(TwoOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
      Assert.assertEquals(null, def.getOutputStreamLabels());
      Assert.assertEquals(StageType.SOURCE, def.getType());
      Assert.assertFalse(def.isVariableOutputStreams());
      Assert.assertFalse(def.hasOnRecordError());
      Assert.assertFalse(def.hasPreconditions());
      Assert.assertTrue(def.getUpgrader() instanceof Source2Upgrader);
      Assert.assertFalse(def.isProducingEvents());
    } finally {
      System.clearProperty(noPrivateCLProperty);
    }
  }

  @Test
  public void testExtractSource3() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Source3.class, "x");
    Assert.assertEquals(0, def.getOutputStreams());
    Assert.assertEquals(StageDef.VariableOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertTrue(def.isVariableOutputStreams());
  }

  @Test
  public void testExtractTarget1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Target1.class, "x");
    Assert.assertEquals(StageType.TARGET, def.getType());
    Assert.assertEquals(0, def.getOutputStreams());
    Assert.assertEquals(null, def.getOutputStreamLabelProviderClass());
    Assert.assertTrue(def.hasOnRecordError());
    Assert.assertFalse(def.hasPreconditions());
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_REQUIRED_FIELDS_CONFIG));
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_PRECONDITIONS_CONFIG));
    Assert.assertTrue(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_ON_RECORD_ERROR_CONFIG));
  }

  @Test
  public void testExtractTarget2() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Target2.class, "x");
    Assert.assertTrue(def.hasOnRecordError());
    Assert.assertTrue(def.hasPreconditions());
    Assert.assertTrue(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_REQUIRED_FIELDS_CONFIG));
    Assert.assertTrue(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_PRECONDITIONS_CONFIG));
    Assert.assertTrue(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_ON_RECORD_ERROR_CONFIG));
  }

  @Test
  public void testExtractExecutor1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Executor1.class, "x");
    Assert.assertEquals(StageType.EXECUTOR, def.getType());
    Assert.assertEquals(0, def.getOutputStreams());
  }

  @Test
  public void testExtractHiveNonExistingConfigTarget() {
    try {
      StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, HideNonExistingConfigTarget.class, "x");
      Assert.fail("Should fail on hiding non-existing config.");
    } catch(IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage().contains("is hiding non-existing config this.config.does.not.exists"));
    }
  }

  @Test
  public void testExtractToErrorTarget1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, ToErrorTarget1.class, "x");
    Assert.assertEquals(StageType.TARGET, def.getType());
    Assert.assertEquals(0, def.getOutputStreams());
    Assert.assertEquals(null, def.getOutputStreamLabelProviderClass());
    Assert.assertFalse(def.hasOnRecordError());
    Assert.assertFalse(def.hasPreconditions());
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_REQUIRED_FIELDS_CONFIG));
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_PRECONDITIONS_CONFIG));
    Assert.assertFalse(def.getConfigDefinitionsMap().containsKey(StageConfigBean.STAGE_ON_RECORD_ERROR_CONFIG));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractMissingIcon() {
    StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, MissingIcon.class, "x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonTargetOffsetCommit() {
    StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, OffsetCommitSource.class, "x");
  }

  @Test
  public void testExtractPipelineLifecycleStage() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, PipelineLifecycleTarget.class, "x");
    Assert.assertEquals(StageType.TARGET, def.getType());
    Assert.assertTrue(def.isPipelineLifecycleStage());
  }

  @Test
  public void testPushOriginMarkedWithOffsetCommitter() {
    try {
      StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, OffsetCommitterPushSource.class, "x");
      Assert.fail("Should fail with invalid use of OffsetCommitter");
    } catch(IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("OffsetCommitter can only be a (Pull) Source"));
    }
  }

  @Test
  public void testLibraryClusterTypes() {
    Properties props = new Properties();
    props.put(StageLibraryDefinition.CLUSTER_CONFIG_CLUSTER_TYPES, "LOCAL,YARN");
    StageLibraryDefinition libDef = new StageLibraryDefinition(
        TestStageDefinitionExtractor.class.getClassLoader(),
        "mock",
        "MOCK",
        props,
        null,
        null,
        null
    );
    Assert.assertEquals(ImmutableList.of(SparkClusterType.LOCAL, SparkClusterType.YARN), libDef.getClusterTypes());
  }

  @Test
  public void testLibraryExecutionOverride() {
    Properties props = new Properties();
    props.put("execution.mode_" + Source1.class.getName(), "CLUSTER_BATCH");
    StageLibraryDefinition libDef = new StageLibraryDefinition(TestStageDefinitionExtractor.class.getClassLoader(),
                                                               "mock", "MOCK", props, null, null, null);

    StageDefinition def = StageDefinitionExtractor.get().extract(libDef, Source1.class, "x");
    Assert.assertEquals(ImmutableList.of(ExecutionMode.CLUSTER_BATCH),def.getExecutionModes());
  }

  @Test
  public void testLibraryExecutionOverrideCloud() {
    try {
      System.setProperty("streamsets.cloud", "true");
      Properties props = new Properties();
      props.put("cloud.execution.mode_" + Source1.class.getName(), "CLUSTER_BATCH");
      StageLibraryDefinition libDef = new StageLibraryDefinition(TestStageDefinitionExtractor.class.getClassLoader(),
          "mock", "MOCK", props, null, null, null
      );

      StageDefinition def = StageDefinitionExtractor.get().extract(libDef, Source1.class, "x");
      Assert.assertEquals(ImmutableList.of(ExecutionMode.CLUSTER_BATCH), def.getExecutionModes());
    } finally {
      System.getProperties().remove("streamsets.cloud");
    }
  }

  @Test
  public void testLibraryExecutionWildcardOverride() {
    Properties props = new Properties();
    props.put(StageLibraryDefinition.getExecutionModePrefix() + StageLibraryDefinition.STAGE_WILDCARD, "CLUSTER_BATCH");
    StageLibraryDefinition libDef = new StageLibraryDefinition(TestStageDefinitionExtractor.class.getClassLoader(),
        "mock", "MOCK", props, null, null, null);

    StageDefinition def = StageDefinitionExtractor.get().extract(libDef, Source1.class, "x");
    Assert.assertEquals(ImmutableList.of(ExecutionMode.CLUSTER_BATCH),def.getExecutionModes());
  }

  @Test
  public void testProducesEvents() {
    Properties props = new Properties();
    props.put(StageLibraryDefinition.getExecutionModePrefix() + ProducesEventsTarger.class.getName(), "CLUSTER_BATCH");
    StageLibraryDefinition libDef = new StageLibraryDefinition(TestStageDefinitionExtractor.class.getClassLoader(),
                                                               "mock", "MOCK", props, null, null, null);

    StageDefinition def = StageDefinitionExtractor.get().extract(libDef, ProducesEventsTarger.class, "x");
    Assert.assertTrue(def.isProducingEvents());
  }

  @Test
  public void testExtractHideStage() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, HiddenStage.class, "x");

    Assert.assertNotNull(def.getHideStage());
    Assert.assertEquals(1, def.getHideStage().size());
    Assert.assertEquals(HideStage.Type.FIELD_PROCESSOR, def.getHideStage().get(0));
  }

  @Test
  public void testExtractYamlUpgrader() {
    StageDefinition def = StageDefinitionExtractor.get().extract(MOCK_LIB_DEF, Source3.class, "x");
    Assert.assertEquals("upgrader/source1.yaml", def.getYamlUpgrader());
  }

}
