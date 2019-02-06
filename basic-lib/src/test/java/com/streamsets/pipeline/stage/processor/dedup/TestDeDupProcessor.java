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
package com.streamsets.pipeline.stage.processor.dedup;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
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
import java.util.LinkedHashMap;
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
    Processor processor = new DeDupProcessor(4, 1, SelectFields.SPECIFIED_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs2() throws Exception {
    Processor processor = new DeDupProcessor(4, 1, SelectFields.SPECIFIED_FIELDS, Arrays.asList("/a"));
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    runner.runDestroy();
  }

  @Test(expected = StageException.class)
  public void testValidateConfigs3() throws Exception {
    Processor processor = new DeDupProcessor(0, 0, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs4() throws Exception {
    Processor processor = new DeDupProcessor(4, 0, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testValidateConfigs6() throws Exception {

    Processor processor = new DeDupProcessor((int) (getDefaultMemoryLimitMiB() * 1000 * 1000 / 85 - 1), 0,
        SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
  }

  @Test
  public void testUniqueSingleBatchAllFields() throws Exception {
    Processor processor = new DeDupProcessor(4, 1, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
  public void testUniqueSingleBatchSpecifiedFields() throws Exception {
    Processor processor = new DeDupProcessor(4, 1, SelectFields.SPECIFIED_FIELDS, Arrays.asList("/value"));
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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

    processor = new DeDupProcessor(4, 1, SelectFields.SPECIFIED_FIELDS, Arrays.asList("/value"));
    runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
  public void testDupWithinTimeWindowSingleBatch() throws Exception {
    Processor processor = new DeDupProcessor(4, 1, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
    Processor processor = new DeDupProcessor(4, 1, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
    Processor processor = new DeDupProcessor(4, 1, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
    Processor processor = new DeDupProcessor(4, 1, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
    Processor processor = new DeDupProcessor(3, 0, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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
    Processor processor = new DeDupProcessor(3, 0, SelectFields.ALL_FIELDS, Collections.EMPTY_LIST);
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
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

  @Test
  public void testWildCardDedup() throws Exception {
    Processor processor = new DeDupProcessor(4, 1, SelectFields.SPECIFIED_FIELDS,
        ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name"));
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecord("a", "b");
      Record r1 = createRecord("b", "a");
      Record r2 = createRecord("c", "c");
      Record r3 = createRecord("a", "b");
      List<Record> input = ImmutableList.of(r0, r1, r2, r3);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(3, output.getRecords().get("unique").size());
      Assert.assertEquals(1, output.getRecords().get("duplicate").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistentField() throws Exception {
    Processor processor =
        new DeDupProcessor(
            4,
            1,
            SelectFields.SPECIFIED_FIELDS,
            ImmutableList.of("/nonExistentField")
        );
    ProcessorRunner runner = new ProcessorRunner.Builder(DeDupDProcessor.class, processor)
        .addOutputLane("unique")
        .addOutputLane("duplicate")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    try {
      Record r = createRecord("a", "b");
      StageRunner.Output output = runner.runProcess(Collections.singletonList(r));
      Assert.assertEquals(0, output.getRecords().get("unique").size());
      Assert.assertEquals(0, output.getRecords().get("duplicate").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Record errorRecord = runner.getErrorRecords().get(0);
      Assert.assertEquals(Errors.DEDUP_04.name(), errorRecord.getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  private Record createRecord(String name, String anotherName) {
    Field name1 = Field.create(name);
    Field name2 = Field.create(anotherName);
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create(name);
    Field name4 = Field.create(anotherName);
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create("madhu");
    Field name6 = Field.create("girish");
    Map<String, Field> nameMap5 = new HashMap<>();
    nameMap5.put("name", name5);
    Map<String, Field> nameMap6 = new HashMap<>();
    nameMap6.put("name", name6);

    Field first = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap1), Field.create(nameMap2)));
    Field second = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap3), Field.create(nameMap4)));
    Field third = Field.create(Field.Type.LIST, ImmutableList.of(Field.create(nameMap5), Field.create(nameMap6)));

    Map<String, Field> noe = new HashMap<>();
    noe.put("streets", Field.create(ImmutableList.of(first, second)));

    Map<String, Field> cole = new HashMap<>();
    cole.put("streets", Field.create(ImmutableList.of(third)));


    Map<String, Field> sfArea = new HashMap<>();
    sfArea.put("noe", Field.create(noe));

    Map<String, Field> utahArea = new HashMap<>();
    utahArea.put("cole", Field.create(cole));


    Map<String, Field> california = new HashMap<>();
    california.put("SanFrancisco", Field.create(sfArea));

    Map<String, Field> utah = new HashMap<>();
    utah.put("SantaMonica", Field.create(utahArea));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("USA", Field.create(Field.Type.LIST,
        ImmutableList.of(Field.create(california), Field.create(utah))));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    return record;
  }

  private long getDefaultMemoryLimitMiB() {
    long maxMemoryMiB = Runtime.getRuntime().maxMemory() / 1000 / 1000;
    return (long)(maxMemoryMiB * 0.65);
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
