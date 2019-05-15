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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestFieldTypeConverterProcessorTypes {

  @Test
  public void testNullRecord() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
     Record record = RecordCreator.create("s", "s:1");

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNullMap() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
     Record record = RecordCreator.create("s", "s:1");
     record.set(Field.create(Field.Type.MAP, null));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBooleanToInt() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.BOOLEAN;
    converterConfig.targetType = Field.Type.INTEGER;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
     Record record = RecordCreator.create("s", "s:1");
     record.set(Field.create(ImmutableMap.of(
       "a", Field.create(true),
       "b", Field.create("String"),
       "c", Field.create(false)
     )));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record outputRecord = output.getRecords().get("a").get(0);

      Assert.assertTrue(outputRecord.has("/a"));
      Assert.assertTrue(outputRecord.has("/b"));
      Assert.assertTrue(outputRecord.has("/c"));
      Assert.assertEquals(Field.Type.INTEGER, outputRecord.get("/a").getType());
      Assert.assertEquals(Field.Type.STRING, outputRecord.get("/b").getType());
      Assert.assertEquals(Field.Type.INTEGER, outputRecord.get("/c").getType());
      Assert.assertEquals(1, outputRecord.get("/a").getValueAsInteger());
      Assert.assertEquals("String", outputRecord.get("/b").getValueAsString());
      Assert.assertEquals(0, outputRecord.get("/c").getValueAsInteger());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRootMap() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("true"));
      map.put("intermediate", Field.create("yes"));
      map.put("skilled", Field.create("122345566"));
      map.put("null", Field.create(Field.Type.STRING, null));
      map.put("number", Field.create(Field.Type.INTEGER, 10));
      map.put("decimal", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1)));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(true, result.get("beginner").getValue());

      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(false, result.get("intermediate").getValue());

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(false, result.get("skilled").getValue());

      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(Field.Type.BOOLEAN, result.get("null").getType());
      Assert.assertEquals(null, result.get("null").getValue());

      Assert.assertTrue(result.containsKey("number"));
      Assert.assertEquals(10, result.get("number").getValue());

      Assert.assertTrue(result.containsKey("decimal"));
      Assert.assertEquals(BigDecimal.valueOf(1), result.get("decimal").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRootList() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      List<Field> list = new LinkedList<>();
      list.add(Field.create("true"));
      list.add(Field.create("yes"));
      list.add(Field.create("122345566"));
      list.add(Field.create(Field.Type.STRING, null));
      list.add(Field.create(Field.Type.INTEGER, 10));
      list.add(Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1)));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(list));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof List);
      List<Field> result = field.getValueAsList();
      Assert.assertTrue(result.size() == 6);

      Assert.assertEquals(true, result.get(0).getValue());

      Assert.assertEquals(false, result.get(1).getValue());

      Assert.assertEquals(false, result.get(2).getValue());

      Assert.assertEquals(Field.Type.BOOLEAN, result.get(3).getType());
      Assert.assertEquals(null, result.get(3).getValue());

      Assert.assertEquals(10, result.get(4).getValue());

      Assert.assertEquals(BigDecimal.valueOf(1), result.get(5).getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMapOfMap() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> innerMap = new LinkedHashMap<>();
      innerMap.put("beginner", Field.create("true"));
      innerMap.put("intermediate", Field.create("yes"));
      innerMap.put("skilled", Field.create("122345566"));
      innerMap.put("null", Field.create(Field.Type.STRING, null));
      innerMap.put("number", Field.create(Field.Type.INTEGER, 10));
      innerMap.put("decimal", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1)));
      Field innerField = Field.create(innerMap);

      Map<String, Field> outerMap = new LinkedHashMap<>();
      outerMap.put("map", innerField);

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(outerMap));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);

      Assert.assertTrue(result.containsKey("map"));
      Assert.assertEquals(Field.Type.MAP, result.get("map").getType());
      result = result.get("map").getValueAsMap();
      Assert.assertTrue(result.size() == 6);

      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(true, result.get("beginner").getValue());

      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(false, result.get("intermediate").getValue());

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(false, result.get("skilled").getValue());

      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(Field.Type.BOOLEAN, result.get("null").getType());
      Assert.assertEquals(null, result.get("null").getValue());

      Assert.assertTrue(result.containsKey("number"));
      Assert.assertEquals(10, result.get("number").getValue());

      Assert.assertTrue(result.containsKey("decimal"));
      Assert.assertEquals(BigDecimal.valueOf(1), result.get("decimal").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testListOfList() throws StageException {
    WholeTypeConverterConfig converterConfig = new WholeTypeConverterConfig();
    converterConfig.sourceType = Field.Type.STRING;
    converterConfig.targetType = Field.Type.BOOLEAN;
    converterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("convertBy", ConvertBy.BY_TYPE)
      .addConfiguration("wholeTypeConverterConfigs", ImmutableList.of(converterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      List<Field> innerList = new LinkedList<>();
      innerList.add(Field.create("true"));
      innerList.add(Field.create("yes"));
      innerList.add(Field.create("122345566"));
      innerList.add(Field.create(Field.Type.STRING, null));
      innerList.add(Field.create(Field.Type.INTEGER, 10));
      innerList.add(Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1)));
      Field innerField = Field.create(innerList);

      List<Field> outerList = new LinkedList<>();
      outerList.add(innerField);

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(outerList));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof List);
      List<Field> result = field.getValueAsList();
      Assert.assertTrue(result.size() == 1);

      field = result.get(0);

      Assert.assertTrue(field.getValue() instanceof List);
      result = field.getValueAsList();
      Assert.assertTrue(result.size() == 6);

      Assert.assertEquals(true, result.get(0).getValue());

      Assert.assertEquals(false, result.get(1).getValue());

      Assert.assertEquals(false, result.get(2).getValue());

      Assert.assertEquals(Field.Type.BOOLEAN, result.get(3).getType());
      Assert.assertEquals(null, result.get(3).getValue());

      Assert.assertEquals(10, result.get(4).getValue());

      Assert.assertEquals(BigDecimal.valueOf(1), result.get(5).getValue());
    } finally {
      runner.runDestroy();
    }
  }

}
