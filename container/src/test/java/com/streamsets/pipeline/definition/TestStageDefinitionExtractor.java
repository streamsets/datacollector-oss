/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigGroupDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;

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

  @StageDef(version = "1", label = "L", description = "D", icon = "I")
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

  public enum TwoOutputStreams implements Label {
    OS1, OS2;

    @Override
    public String getLabel() {
      return name();
    }
  }

  @StageDef(version = "2", label = "LL", description = "DD", icon = "II", execution = ExecutionMode.STANDALONE,
  outputStreams = TwoOutputStreams.class)
  @ConfigGroups(Group1.class)
  @RawSource(rawSourcePreviewer = Previewer.class)
  @HideConfig(value = "config2", preconditions = true, onErrorRecord = true)
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

  @StageDef(version = "1", label = "L", outputStreams = StageDef.VariableOutputStreams.class,
      outputStreamsDrivenByConfig = "config1")
  public static class Source3 extends Source1 {

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @StageDef(version = "1", label = "L")
  public static class Target1 extends BaseTarget {
    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @Test
  public void testExtractSource1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(Source1.class, "x");
    Assert.assertEquals(Source1.class.getName(), def.getClassName());
    Assert.assertEquals(Source1.class.getName(), def.getName());
    Assert.assertEquals("1", def.getVersion());
    Assert.assertEquals("L", def.getLabel());
    Assert.assertEquals("D", def.getDescription());
    Assert.assertEquals(null, def.getRawSourceDefinition());
    Assert.assertEquals(0, def.getConfigGroupDefinition().getGroupNames().size());
    Assert.assertEquals(2, def.getConfigDefinitions().size());
    Assert.assertEquals(1, def.getOutputStreams());
    Assert.assertEquals(2, def.getExecutionModes().size());
    Assert.assertEquals("I", def.getIcon());
    Assert.assertEquals(StageDef.DefaultOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertEquals(null, def.getOutputStreamLabels());
    Assert.assertEquals(StageType.SOURCE, def.getType());
  }

  @Test
  public void testExtractSource2() {
    StageDefinition def = StageDefinitionExtractor.get().extract(Source2.class, "x");
    Assert.assertEquals(Source2.class.getName(), def.getClassName());
    Assert.assertEquals(Source2.class.getName(), def.getName());
    Assert.assertEquals("2", def.getVersion());
    Assert.assertEquals("LL", def.getLabel());
    Assert.assertEquals("DD", def.getDescription());
    Assert.assertNotNull(def.getRawSourceDefinition());
    Assert.assertEquals(1, def.getConfigGroupDefinition().getGroupNames().size());
    Assert.assertEquals(3, def.getConfigDefinitions().size());
    Assert.assertEquals(2, def.getOutputStreams());
    Assert.assertEquals(1, def.getExecutionModes().size());
    Assert.assertEquals("II", def.getIcon());
    Assert.assertEquals(TwoOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertEquals(null, def.getOutputStreamLabels());
    Assert.assertEquals(StageType.SOURCE, def.getType());
    Assert.assertFalse(def.isVariableOutputStreams());
    Assert.assertTrue(def.hasOnRecordError());
    Assert.assertTrue(def.hasPreconditions());
  }

  @Test
  public void testExtractSource3() {
    StageDefinition def = StageDefinitionExtractor.get().extract(Source3.class, "x");
    Assert.assertEquals(0, def.getOutputStreams());
    Assert.assertEquals(StageDef.VariableOutputStreams.class.getName(), def.getOutputStreamLabelProviderClass());
    Assert.assertTrue(def.isVariableOutputStreams());
  }

  @Test
  public void testExtractTarget1() {
    StageDefinition def = StageDefinitionExtractor.get().extract(Target1.class, "x");
    Assert.assertEquals(StageType.TARGET, def.getType());
    Assert.assertEquals(0, def.getOutputStreams());
    Assert.assertEquals(null, def.getOutputStreamLabelProviderClass());
  }

}
