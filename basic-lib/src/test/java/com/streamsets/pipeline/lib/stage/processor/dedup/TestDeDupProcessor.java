/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.dedup;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDeDupProcessor {

  private Record createRecordWithValue(String value) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("value", Field.create(value));
    record.set(Field.create(map));
    return record;
  }

  @Test
  public void testUniqueSingleBatch() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      Record r1 = createRecordWithValue("b");
      Record r2 = createRecordWithValue("c");
      Record r3 = createRecordWithValue("d");
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(4, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDupWithinTimeWindowSingleBatch() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      Record r1 = createRecordWithValue("b");
      Record r2 = createRecordWithValue("c");
      Record r3 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(3, output.getRecords().get("unique").size());
      Assert.assertEquals(1, output.getRecords().get("duplicate").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUniqueMultipleBatches() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("b");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("c");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("d");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDupWithinTimeWindowMultipleBatches() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("b");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("c");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("a");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(0, output.getRecords().get("unique").size());
      Assert.assertEquals(1, output.getRecords().get("duplicate").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDupOutsideTimeWindowMultipleBatches() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("b");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("c");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      Thread.sleep(1001);

      r0 = createRecordWithValue("a");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDupWithinRecordTailSingleBatch() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", 3)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      Record r1 = createRecordWithValue("b");
      Record r2 = createRecordWithValue("c");
      Record r3 = createRecordWithValue("d");
      Record r4 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(5, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDupWithinRecordTailMultipleBatches() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("hashAllFields", true)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", 3)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithValue("a");
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("b");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("c");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("d");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

      r0 = createRecordWithValue("a");
      input = ImmutableList.of(r0);
      output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());

    } finally {
      runner.runDestroy();
    }
  }

}
