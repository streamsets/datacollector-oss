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
        .addConfiguration("separator", "\\^")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of())
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
        .addOutputLane("out")
        .build();
    Assert.assertEquals(1, runner.runValidateConfigs().size());
    Assert.assertTrue(runner.runValidateConfigs().get(0).toString().contains("SPLITTER_00"));
  }

  @Test
  public void testSplitting() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
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
  public void testSplittingToList() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LIST)
        .addConfiguration("remainingSplitsPath", "/splits_list")
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
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/splits_list").getValueAsList().get(0).getValue());
      Assert.assertEquals("a", output.getRecords().get("out").get(1).get("/a").getValue());
      Assert.assertEquals(2, output.getRecords().get("out").get(1).get("/splits_list").getValueAsList().size());
      Assert.assertEquals("b", output.getRecords().get("out").get(1).get("/splits_list").getValueAsList().get(0).getValue());
      Assert.assertEquals("c", output.getRecords().get("out").get(1).get("/splits_list").getValueAsList().get(1).getValue());
      Assert.assertEquals("a", output.getRecords().get("out").get(2).get("/a").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(2).has("/splits_list"));
      Assert.assertEquals("", output.getRecords().get("out").get(3).get("/a").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(3).has("/splits_list"));
      Assert.assertEquals(null, output.getRecords().get("out").get(4).get("/a").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(4).has("/splits_list"));
      Assert.assertEquals(null, output.getRecords().get("out").get(5).get("/a").getValue());
      Assert.assertFalse(output.getRecords().get("out").get(5).has("/splits_list"));
    } finally {
      runner.runDestroy();
    }

    runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of())
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LIST)
        .addConfiguration("remainingSplitsPath", "/splits_list")
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine("a b c");
      Record r1 = createRecordWithLine("");
      Record r2 = createRecordWithLine(" ");
      Record r3 = createRecordWithLine(null);
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(4, output.getRecords().get("out").size());
      Assert.assertEquals(3, output.getRecords().get("out").get(0).get("/splits_list").getValueAsList().size());
      Assert.assertEquals("a", output.getRecords().get("out").get(0).get("/splits_list").getValueAsList().get(0).getValue());
      Assert.assertEquals("b", output.getRecords().get("out").get(0).get("/splits_list").getValueAsList().get(1).getValue());
      Assert.assertEquals("c", output.getRecords().get("out").get(0).get("/splits_list").getValueAsList().get(2).getValue());
      Assert.assertEquals("", output.getRecords().get("out").get(1).get("/splits_list").getValueAsList().get(0).getValue());
      Assert.assertFalse(output.getRecords().get("out").get(2).has("/splits_list"));
      Assert.assertFalse(output.getRecords().get("out").get(3).has("/splits_list"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSplittingToError() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
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
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
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
        .addConfiguration("separator", " ")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
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
      .addConfiguration("separator", " ")
      .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addConfiguration("originalFieldAction", OriginalFieldAction.KEEP)
      .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
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

  @Test
  public void testSplittingWithSymbol() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", "\\^")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {
      Record r0 = createRecordWithLine("test^test");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals("test", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("test", output.getRecords().get("out").get(0).get("/b").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSplittingWithSymbol2() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(SplitterDProcessor.class)
        .addConfiguration("fieldPath", "/line")
        .addConfiguration("separator", "\\*")
        .addConfiguration("fieldPathsForSplits", ImmutableList.of("/a", "/b"))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("originalFieldAction", OriginalFieldAction.REMOVE)
        .addConfiguration("tooManySplitsAction", TooManySplitsAction.TO_LAST_FIELD)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {
      Record r0 = createRecordWithLine("test*test");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals("test", output.getRecords().get("out").get(0).get("/a").getValue());
      Assert.assertEquals("test", output.getRecords().get("out").get(0).get("/b").getValue());
    } finally {
      runner.runDestroy();
    }
  }
}
