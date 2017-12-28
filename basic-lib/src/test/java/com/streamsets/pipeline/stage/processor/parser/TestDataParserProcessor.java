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

package com.streamsets.pipeline.stage.processor.parser;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatParserService;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestDataParserProcessor {

  @Test
  public void parseSimpleValue() throws Exception {
    DataParserConfig config = new DataParserConfig();
    config.fieldPathToParse = "/";
    config.multipleValuesBehavior = MultipleValuesBehavior.FIRST_ONLY;
    config.parsedFieldPath = "/";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, new DataParserProcessor(config))
      .addOutputLane("out")
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .build();

    runner.runInit();
    try {
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create("{\"key\": \"value\"}"));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      List<Record> outputRecords = output.getRecords().get("out");
      assertEquals(1, outputRecords.size());

      Record outputRecord = outputRecords.get(0);
      assertTrue(outputRecord.has("/"));
      assertTrue(outputRecord.has("/key"));

      Field field = outputRecord.get("/key");
      assertEquals(Field.Type.STRING, field.getType());
      assertEquals("value", field.getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void parseMultipleValuesSplitIntoRecords() throws Exception {
    DataParserConfig config = new DataParserConfig();
    config.fieldPathToParse = "/";
    config.multipleValuesBehavior = MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS;
    config.parsedFieldPath = "/";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, new DataParserProcessor(config))
      .addOutputLane("out")
      .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
      .build();

    runner.runInit();
    try {
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create("{\"first\": 1}\n{\"second\": 2}"));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      List<Record> outputRecords = output.getRecords().get("out");
      assertEquals(2, outputRecords.size());

      Record outputRecord = outputRecords.get(0);
      assertTrue(outputRecord.has("/first"));
      Field field = outputRecord.get("/first");
      assertEquals(Field.Type.INTEGER, field.getType());
      assertEquals(1, field.getValueAsInteger());

      outputRecord = outputRecords.get(1);
      assertTrue(outputRecord.has("/second"));
      field = outputRecord.get("/second");
      assertEquals(Field.Type.INTEGER, field.getType());
      assertEquals(2, field.getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidInputField() throws Exception {
    String inputFieldPath = "input";
    String outputFieldPath = "output";

    DataParserConfig configs = new DataParserConfig();
    configs.fieldPathToParse = "/" + inputFieldPath + "_non_existent";
    configs.parsedFieldPath = "/" + outputFieldPath;
    configs.multipleValuesBehavior = MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS;

    DataParserProcessor processor = new DataParserProcessor(configs);

    final String outputLane = "out";

    ProcessorRunner runner = new ProcessorRunner.Builder(DataParserDProcessor.class, processor)
        .addOutputLane(outputLane)
        .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    List<Record> input = new ArrayList<>();

    Map<String, Field> map = new HashMap<>();
    map.put(inputFieldPath, Field.create("{\"first\": 1}"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().containsKey(outputLane));
      final List<Record> records = output.getRecords().get(outputLane);
      assertThat(records, hasSize(0));
      final List<Record> errors = runner.getErrorRecords();
      assertThat(errors, notNullValue());
      assertThat(errors, hasSize(1));
      Record errorRecord = errors.get(0);
      assertThat(errorRecord.getHeader().getErrorCode(), equalTo(Errors.DATAPARSER_05.name()));
    } finally {
      runner.runDestroy();
    }
  }
}
