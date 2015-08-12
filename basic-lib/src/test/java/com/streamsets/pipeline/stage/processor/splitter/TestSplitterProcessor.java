/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.splitter;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSplitterProcessor {

  private Record createRecordWithLine(String line) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("line", Field.create(line));
    record.set(Field.create(map));
    return record;
  }

  @Test
  public void testValidateConfigs() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", '^')
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addOutputLane("out")
        .build();
    Assert.assertEquals(1, runner.runValidateConfigs().size());
    Assert.assertTrue(runner.runValidateConfigs().get(0).toString().contains("SPLITTER_00"));
  }

  @Test
  public void testSplitting() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", '^')
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b");
      Record r1 = createRecordWithLine("a b c");
      Record r2 = createRecordWithLine("a");
      Record r3 = createRecordWithLine("");
      Record r4 = createRecordWithLine(" ");
      Record r5 = createRecordWithLine(null);
      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4, r5);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(6, output.getRecords().get("out").size());
      Assert.assertEquals("a", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/b").getValue());
      Assert.assertEquals("a", output.getRecords().get("out").get(1).get("/a").getValue());
      Assert.assertEquals("b c", output.getRecords().get("out").get(1).get("/b").getValue());
      Assert.assertEquals("a", output.getRecords().get("out").get(2).get("/a").getValue());
      Assert.assertEquals(null, output.getRecords().get("out").get(2).get("/b").getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(3).get("/a").getValue());
      Assert.assertEquals(null, output.getRecords().get("out").get(3).get("/b").getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(4).get("/a").getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(4).get("/b").getValue());
      Assert.assertEquals(null, output.getRecords().get("out").get(5).get("/a").getValue());
      Assert.assertEquals(null, output.getRecords().get("out").get(5).get("/b").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSplittingToError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", '^')
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b");
      Record r1 = createRecordWithLine("a b c");
      Record r2 = createRecordWithLine("a");
      Record r3 = createRecordWithLine("");
      Record r4 = createRecordWithLine(" ");
      Record r5 = createRecordWithLine(null);
      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4, r5);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(3, output.getRecords().get("out").size());
      Assert.assertEquals("a", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/b").getValue());
      Assert.assertEquals("a", output.getRecords().get("out").get(1).get("/a").getValue());
      Assert.assertEquals("b c", output.getRecords().get("out").get(1).get("/b").getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(2).get("/a").getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(2).get("/b").getValue());
      Assert.assertEquals(3, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testKeepUnplitValue() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", '^')
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertEquals("a", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/b").getValue());
      Assert.assertEquals("a b", output.getRecords().get("out").get(0).get("/line").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveUnplitValue() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", '^')
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertEquals("a", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/b").getValue());
      Assert.assertNull(output.getRecords().get("out").get(0).get("/line"));
    } finally {
      runner.runDestroy();
    }

  }

  @Test(expected = OnRecordErrorException.class)
  public void testSplittingNonStringField() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
      .addConfiguration("fieldPath", "/")
      .addConfiguration("separator", '^')
      .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
      .addOutputLane("out")
      .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b");
      List<Record> input = ImmutableList.of(r0);
      runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }

}
