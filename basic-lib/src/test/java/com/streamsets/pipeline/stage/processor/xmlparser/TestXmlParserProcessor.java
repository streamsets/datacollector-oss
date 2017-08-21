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
package com.streamsets.pipeline.stage.processor.xmlparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestXmlParserProcessor {

  @Test
  public void testParsing() throws Exception {
    XmlParserConfig configs = new XmlParserConfig();
    configs.charset = "UTF-8";
    configs.fieldPathToParse = "/xml";
    configs.parsedFieldPath = "/data";
    configs.removeCtrlChars = false;
    configs.xmlRecordElement = "a";

    XmlParserDProcessor processor = new XmlParserDProcessor();
    processor.configs = configs;

    ProcessorRunner runner = new ProcessorRunner.Builder(XmlParserDProcessor.class, processor)
        .addOutputLane("out").setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put("xml", Field.create("<root><a>A</a></root>"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertTrue(output.getRecords().containsKey("out"));
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertEquals("A", output.getRecords().get("out").get(0).get("/data/value").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingInvalidXmlRecord() throws Exception {
    XmlParserConfig configs = new XmlParserConfig();
    configs.charset = "UTF-8";
    configs.fieldPathToParse = "/xml";
    configs.parsedFieldPath = "/data";
    configs.removeCtrlChars = false;
    configs.xmlRecordElement = "a";

    XmlParserDProcessor processor = new XmlParserDProcessor();
    processor.configs = configs;

    ProcessorRunner runner = new ProcessorRunner.Builder(XmlParserDProcessor.class, processor)
        .addOutputLane("out").setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put("xml", Field.create("<root><a>A"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertTrue(output.getRecords().containsKey("out"));
      Assert.assertEquals(0, output.getRecords().get("out").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingInvalidDelimiter() throws Exception {
    XmlParserConfig configs = new XmlParserConfig();
    configs.charset = "UTF-8";
    configs.fieldPathToParse = "/xml";
    configs.parsedFieldPath = "/data";
    configs.removeCtrlChars = false;
    configs.xmlRecordElement = "WRONG_DELIMITER"; // Delimiter element does not exist!

    XmlParserDProcessor processor = new XmlParserDProcessor();
    processor.configs = configs;

    ProcessorRunner runner = new ProcessorRunner.Builder(XmlParserDProcessor.class, processor)
        .addOutputLane("out").setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put("xml", Field.create("<root><a>A</a></root>"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertTrue(output.getRecords().containsKey("out"));
      Assert.assertEquals(1, output.getRecords().get("out").size());
      Assert.assertNull(output.getRecords().get("out").get(0).get("/data/value"));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingMultipleValuesAsList() throws Exception {
    XmlParserConfig configs = new XmlParserConfig();
    configs.charset = "UTF-8";
    configs.fieldPathToParse = "/xml";
    configs.parsedFieldPath = "/data";
    configs.removeCtrlChars = false;
    configs.xmlRecordElement = "a";
    configs.multipleValuesBehavior = MultipleValuesBehavior.ALL_AS_LIST;

    XmlParserDProcessor processor = new XmlParserDProcessor();
    processor.configs = configs;

    ProcessorRunner runner = new ProcessorRunner.Builder(XmlParserDProcessor.class, processor)
        .addOutputLane("out").setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put("xml", Field.create("<root><a>1</a><a>2</a><a>3</a></root>"));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertTrue(output.getRecords().containsKey("out"));
      final List<Record> records = output.getRecords().get("out");
      Assert.assertEquals(1, records.size());
      final Field outputField = records.get(0).get("/data");
      Assert.assertNotNull(outputField);
      final List<Field> valueList = outputField.getValueAsList();
      Assert.assertNotNull(valueList);
      Assert.assertEquals(3, valueList.size());
      Assert.assertEquals("1", valueList.get(0).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("2", valueList.get(1).getValueAsMap().get("value").getValueAsString());
      Assert.assertEquals("3", valueList.get(2).getValueAsMap().get("value").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParsingMultipleValuesAsSplitRecords() throws Exception {
    XmlParserConfig configs = new XmlParserConfig();
    configs.charset = "UTF-8";
    configs.fieldPathToParse = "/xml";
    configs.parsedFieldPath = "/data";
    configs.removeCtrlChars = false;
    configs.xmlRecordElement = "a";
    configs.multipleValuesBehavior = MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS;

    XmlParserDProcessor processor = new XmlParserDProcessor();
    processor.configs = configs;

    ProcessorRunner runner = new ProcessorRunner.Builder(XmlParserDProcessor.class, processor)
        .addOutputLane("out").setOnRecordError(OnRecordError.TO_ERROR).build();
    Map<String, Field> map = new HashMap<>();
    map.put("xml", Field.create("<root><a>1</a><a>2</a><a>3</a></root>"));
    map.put("first", Field.create(1));
    map.put("second", Field.create(Arrays.asList(Field.create("val1"), Field.create("val2"), Field.create("val3"))));
    Record record = RecordCreator.create();
    record.set(Field.create(map));
    List<Record> input = new ArrayList<>();
    input.add(record);
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertTrue(output.getRecords().containsKey("out"));
      final List<Record> records = output.getRecords().get("out");
      Assert.assertEquals(3, records.size());

      for (int i=0; i<records.size(); i++) {
        assertCommonFields(records.get(i), String.valueOf(i+1));
      }

    } finally {
      runner.runDestroy();
    }
  }

  private void assertCommonFields(Record outRecord, String expectedDataValue) {
    Assert.assertEquals(expectedDataValue, outRecord.get("/data").getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals(1, outRecord.get("/first").getValueAsInteger());
    final List<Field> originalList = outRecord.get("/second").getValueAsList();
    Assert.assertNotNull(originalList);
    Assert.assertEquals(3, originalList.size());
    Assert.assertEquals("val1", originalList.get(0).getValueAsString());
    Assert.assertEquals("val2", originalList.get(1).getValueAsString());
    Assert.assertEquals("val3", originalList.get(2).getValueAsString());
  }

}
