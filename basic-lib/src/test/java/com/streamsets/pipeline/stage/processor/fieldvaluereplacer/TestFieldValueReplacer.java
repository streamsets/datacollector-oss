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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldValueReplacer {

  @Test
  public void testNonExistingFieldsContinue() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/nonExisting");
    nameReplacement.newValue = "StreamSets";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/nonExisting"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(null, result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingFieldsError() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/nonExisting");
    nameReplacement.newValue = "StreamSets";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("fieldsToNull", ImmutableList.of("/nonExisting"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNullConfiguration() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(null, result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEmptyConfiguration() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", new ArrayList<>())
      .addConfiguration("fieldsToReplaceIfNull", new ArrayList<>())
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(null, result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldsToNull() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create("c"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(null, result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(null, result.get("age").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals("c", result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullStringFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/stringField");
    stringFieldReplacement.newValue = "StreamSets";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("stringField", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("stringField"));
      Assert.assertEquals("StreamSets", result.get("stringField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullIntegerFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/intField");
    stringFieldReplacement.newValue = "123";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("intField", Field.create(Field.Type.INTEGER, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("intField"));
      Assert.assertEquals(123, result.get("intField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullDoubleFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/doubleField");
    stringFieldReplacement.newValue = "12345.6789";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("doubleField", Field.create(Field.Type.DOUBLE, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("doubleField"));
      Assert.assertEquals(12345.6789, result.get("doubleField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullLongFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/longField");
    stringFieldReplacement.newValue = "523456789345";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("longField", Field.create(Field.Type.LONG, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("longField"));
      Assert.assertEquals(523456789345L, result.get("longField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullShortFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/shortField");
    stringFieldReplacement.newValue = "32762";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("shortField", Field.create(Field.Type.SHORT, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("shortField"));
      Assert.assertEquals((short)32762, result.get("shortField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullFloatFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/floatField");
    stringFieldReplacement.newValue = "10.345f";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("floatField", Field.create(Field.Type.FLOAT, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("floatField"));
      Assert.assertEquals(10.345f, result.get("floatField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullDecimalFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/decimalField");
    stringFieldReplacement.newValue = "12345678";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("decimalField", Field.create(Field.Type.DECIMAL, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("decimalField"));
      Assert.assertEquals(new BigDecimal(12345678), result.get("decimalField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullByteFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/byteField");
    stringFieldReplacement.newValue = "123";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteField", Field.create(Field.Type.BYTE, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("byteField"));
      Assert.assertEquals((byte)123, result.get("byteField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullByteArrayFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/byteArrayField");
    stringFieldReplacement.newValue = "streamsets";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteArrayField", Field.create(Field.Type.BYTE_ARRAY, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("byteArrayField"));
      Assert.assertTrue(Arrays.equals("streamsets".getBytes(), result.get("byteArrayField").getValueAsByteArray()));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullCharFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/charField");
    stringFieldReplacement.newValue = "c";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("charField", Field.create(Field.Type.CHAR, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("charField"));
      Assert.assertEquals('c', result.get("charField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullBooleanFields() throws StageException {

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/booleanField");
    stringFieldReplacement.newValue = "true";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("booleanField", Field.create(Field.Type.BOOLEAN, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("booleanField"));
      Assert.assertEquals(true, result.get("booleanField").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  /*@Test
  public void testReplaceNullDateFields() throws StageException {

    FieldValueReplacer.FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacer.FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/dateField");
    stringFieldReplacement.newValue = "32762";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacer.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("dateField", Field.create(Field.Type.SHORT, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("dateField"));
      Assert.assertEquals((short)32762, result.get("dateField").getValue());
    } finally {
      runner.runDestroy();
    }
  }*/

  @Test
  public void testFieldsToNullAndReplaceNulls() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/name");
    nameReplacement.newValue = "StreamSets";

    FieldValueReplacerConfig addressReplacement = new FieldValueReplacerConfig();
    addressReplacement.fields = ImmutableList.of("/streetAddress");
    addressReplacement.newValue = "Sansome Street";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement, addressReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("StreamSets", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(null, result.get("age").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals("Sansome Street", result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonNullFields() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/name");
    nameReplacement.newValue = "NewValue";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, "streamsets"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("streamsets", result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardReplacement() throws StageException {

    Field name1 = Field.create("jon");
    Field name2 = Field.create("natty");
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create("adam");
    Field name4 = Field.create("hari");
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create(Field.Type.STRING, null);
    Field name6 = Field.create(Field.Type.STRING, null);
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

    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "jon");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "natty");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "adam");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "hari");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), null);
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), null);

    /* All the field Paths in the record are
        /USA
        /USA[0]
        /USA[0]/SantaMonica
        /USA[0]/SantaMonica/noe
        /USA[0]/SantaMonica/noe/streets
        /USA[0]/SantaMonica/noe/streets[0]
        /USA[0]/SantaMonica/noe/streets[0][0]
        /USA[0]/SantaMonica/noe/streets[0][0]/name
        /USA[0]/SantaMonica/noe/streets[0][1]
        /USA[0]/SantaMonica/noe/streets[0][1]/name
        /USA[0]/SantaMonica/noe/streets[1]
        /USA[0]/SantaMonica/noe/streets[1][0]
        /USA[0]/SantaMonica/noe/streets[1][0]/name
        /USA[0]/SantaMonica/noe/streets[1][1]
        /USA[0]/SantaMonica/noe/streets[1][1]/name
        /USA[1]
        /USA[1]/SantaMonica
        /USA[1]/SantaMonica/cole
        /USA[1]/SantaMonica/cole/streets
        /USA[1]/SantaMonica/cole/streets[0]
        /USA[1]/SantaMonica/cole/streets[0][0]
        /USA[1]/SantaMonica/cole/streets[0][0]/name
        /USA[1]/SantaMonica/cole/streets[0][1]
        /USA[1]/SantaMonica/cole/streets[0][1]/name
      */

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/USA[*]/SantaMonica/*/streets[*][*]/name");
    nameReplacement.newValue = "Razor";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();


    try {

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), null);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), null);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), null);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), null);
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "Razor");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "Razor");

    } finally {
      runner.runDestroy();
    }
  }
}
