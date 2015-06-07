/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.config.ConfigGroupDefinition;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigGroupExtractor {

  public class InvalidGroup implements Label {

    @Override
    public String getLabel() {
      return "invalid";
    }
  }

  @ConfigGroups(InvalidGroup.class)
  public static class InvalidSource extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public enum Group1 implements Label {
    G1;

    @Override
    public String getLabel() {
      return "g1";
    }
  }

  public enum Group2 implements Label {
    G2;

    @Override
    public String getLabel() {
      return "g2";
    }
  }

  @ConfigGroups(Group1.class)
  public static class Source1 extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @ConfigGroups(Group2.class)
  public static class Source2 extends Source1 {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractInvalidGroup() {
    ConfigGroupExtractor.get().extract(InvalidSource.class, "x");
  }

  @Test
  public void testExtractGroupsSource1() {
    ConfigGroupDefinition def = ConfigGroupExtractor.get().extract(Source1.class, "x");
    Assert.assertEquals(ImmutableSet.of("G1"), def.getGroupNames());
    Assert.assertEquals(1, def.getClassNameToGroupsMap().size());
    Assert.assertEquals(1, def.getGroupNameToLabelMapList().size());
    Assert.assertEquals(ImmutableList.of("G1"), def.getClassNameToGroupsMap().get(Group1.class.getName()));
    Assert.assertEquals("G1", def.getGroupNameToLabelMapList().get(0).get("name"));
    Assert.assertEquals("g1", def.getGroupNameToLabelMapList().get(0).get("label"));
  }

  @Test
  public void testExtractGroupsSource2() {
    ConfigGroupDefinition def = ConfigGroupExtractor.get().extract(Source2.class, "x");
    Assert.assertEquals(ImmutableSet.of("G1", "G2"), def.getGroupNames());
    Assert.assertEquals(2, def.getClassNameToGroupsMap().size());
    Assert.assertEquals(2, def.getGroupNameToLabelMapList().size());
    Assert.assertEquals(ImmutableList.of("G1"), def.getClassNameToGroupsMap().get(Group1.class.getName()));
    Assert.assertEquals(ImmutableList.of("G2"), def.getClassNameToGroupsMap().get(Group2.class.getName()));
    Assert.assertEquals("G1", def.getGroupNameToLabelMapList().get(0).get("name"));
    Assert.assertEquals("g1", def.getGroupNameToLabelMapList().get(0).get("label"));
    Assert.assertEquals("G2", def.getGroupNameToLabelMapList().get(1).get("name"));
    Assert.assertEquals("g2", def.getGroupNameToLabelMapList().get(1).get("label"));
  }

}
