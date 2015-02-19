/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.dedup;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
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

  @Test(expected = StageException.class)
  public void testValidateConfigs1() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.SPECIFIED_FIELDS)
        .addConfiguration("fieldsToCompare", Collections.emptyList())
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs2() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.SPECIFIED_FIELDS)
        .addConfiguration("fieldsToCompare", Arrays.asList("/a"))
        .addConfiguration("timeWindowSecs", 1)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testValidateConfigs3() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", 0)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs4() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", 4)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test(expected = StageException.class)
  public void testValidateConfigs5() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", (int) (Runtime.getRuntime().maxMemory() / 3 / 85 + 1))
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs6() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
        .addConfiguration("timeWindowSecs", 0)
        .addConfiguration("recordCountWindow", (int) (Runtime.getRuntime().maxMemory() / 3 / 85 - 1))
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testUniqueSingleBatch() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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
        .addConfiguration("compareFields", SelectFields.ALL_FIELDS)
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

// //TO TEST MEMORY USAGE
//  private final static int MAX_RECORDS = 10000000;
//  @Test
//  public void testMemoryUsage() throws Exception {
//    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupProcessor.class)
//        .addConfiguration("hashAllFields", true)
//        .addConfiguration("timeWindowSecs", 60 * 1000)
//        .addConfiguration("recordCountWindow", MAX_RECORDS)
//        .addOutputLane("unique")
//        .addOutputLane("duplicate")
//        .build();
//    runner.runInit();
//    try {
//      Record record = RecordCreator.create();
//      for (int i = 0; i < MAX_RECORDS; i++ ) {
//        record.set(Field.create(i));
//        ((DeDupProcessor)runner.getStage()).duplicateCheck(record);
//      }
//      System.out.println("XXXX");
//      Thread.sleep(10000);
//    } finally {
//      runner.runDestroy();
//    }
//  }

}
