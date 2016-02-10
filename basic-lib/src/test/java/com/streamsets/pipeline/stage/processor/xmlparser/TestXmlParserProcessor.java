/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
}
