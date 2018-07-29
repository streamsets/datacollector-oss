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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static com.streamsets.testing.Matchers.fieldWithValue;
public class TestFieldValueReplacer {

  @Test
  public void testNonExistingFieldsContinue() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/nonExisting");
    nameReplacement.newValue = "StreamSets";

    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/nonExisting");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
        .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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

    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/nonExisting");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", new ArrayList<>())
      .addConfiguration("fieldsToReplaceIfNull", new ArrayList<>())
        .addConfiguration("fieldsToConditionallyReplace", new ArrayList<>())
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
    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/age");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
      .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteField", Field.create(Field.Type.BYTE, null));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      Assert.assertNotNull(runner);
      Assert.assertNotNull(record);
      Assert.assertNotNull(ImmutableList.of(record));

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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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

  public void testReplaceNullDateTimeTypesFields(Field.Type type, SimpleDateFormat format) throws Exception {
    Date d = format.parse(format.format(new Date()));

    FieldValueReplacerConfig stringFieldReplacement = new FieldValueReplacerConfig();
    stringFieldReplacement.fields = ImmutableList.of("/dateField");
    stringFieldReplacement.newValue = format.format(d);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
      .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("dateField", Field.create(type, null));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record outputRecord = output.getRecords().get("a").get(0);
      Assert.assertTrue(outputRecord.has("/dateField"));
      Field field = outputRecord.get("/dateField");
      Assert.assertEquals(type, field.getType());
      Assert.assertEquals(d, field.getValueAsDatetime());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testReplaceNullDateFields() throws Exception {
    testReplaceNullDateTimeTypesFields(Field.Type.DATE, new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH));
  }
  @Test
  public void testReplaceNullTimeFields() throws Exception {
    testReplaceNullDateTimeTypesFields(Field.Type.TIME, new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH));
  }
  @Test
  public void testReplaceNullDateTimeFields() throws Exception {
    testReplaceNullDateTimeTypesFields(Field.Type.DATETIME, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ", Locale.ENGLISH));
  }

  @Test
  public void testFieldsToNullAndReplaceNulls() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/name");
    nameReplacement.newValue = "StreamSets";

    FieldValueReplacerConfig addressReplacement = new FieldValueReplacerConfig();
    addressReplacement.fields = ImmutableList.of("/streetAddress");
    addressReplacement.newValue = "Sansome Street";

    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/age");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement, addressReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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
      .addConfiguration("nullReplacerConditionalConfigs", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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

    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
      .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
        .addConfiguration("fieldsToConditionallyReplace", null)
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

  @Test
  public void testFieldsToConditionallyReplace() throws StageException {

    FieldValueConditionalReplacerConfig repl = new FieldValueConditionalReplacerConfig();
    repl.operator = "LESS_THAN";
    repl.comparisonValue = "300";
    repl.replacementValue = "100";
    repl.fieldNames = new ArrayList<>();
    repl.fieldNames.add("/money");

    FieldValueConditionalReplacerConfig repl2 = new FieldValueConditionalReplacerConfig();
    repl2.operator = "EQUALS";
    repl2.comparisonValue = "Steve";
    repl2.replacementValue = "Marsha";
    repl2.fieldNames = new ArrayList<>();
    repl2.fieldNames.add("/name");

    FieldValueConditionalReplacerConfig repl3 = new FieldValueConditionalReplacerConfig();
    repl3.operator = "LESS_THAN";
    repl3.comparisonValue = "40.0";
    repl3.replacementValue = "50.5";
    repl3.fieldNames = new ArrayList<>();
    repl3.fieldNames.add("/stateTax");
    repl3.fieldNames.add("/fedTax");

    ArrayList<FieldValueConditionalReplacerConfig> toReplace = new ArrayList<>();
    toReplace.add(repl);
    toReplace.add(repl2);
    toReplace.add(repl3);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs", null)
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", toReplace)
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {
      Record record = getConditionallyReplaceRecord();

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 5);

      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("Marsha", result.get("name").getValue());

      Assert.assertTrue(result.containsKey("money"));
      Assert.assertEquals(100, result.get("money").getValue());

      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals("123 Street", result.get("streetAddress").getValue());

      Assert.assertTrue(result.containsKey("stateTax"));
      Assert.assertEquals(50.5, result.get("stateTax").getValue());

      Assert.assertTrue(result.containsKey("fedTax"));
      Assert.assertEquals(50.5, result.get("fedTax").getValue());

    } finally {
      runner.runDestroy();
    }
  }

  @NotNull
  protected static Record getConditionallyReplaceRecord() {
    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "Steve"));
    map.put("money", Field.create(Field.Type.INTEGER, 200));
    map.put("streetAddress", Field.create(Field.Type.STRING, "123 Street"));
    map.put("stateTax", Field.create(Field.Type.DOUBLE,  7.5));
    map.put("fedTax", Field.create(Field.Type.DOUBLE, 30.34));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    return record;
  }


  @Test
  public void testFieldsToConditionallyReplaceInNestedMap() throws StageException {
    FieldValueConditionalReplacerConfig repl = new FieldValueConditionalReplacerConfig();
    repl.operator = "LESS_THAN";
    repl.comparisonValue = "20";
    repl.replacementValue = "3";
    repl.fieldNames = new ArrayList<>();
    repl.fieldNames.add("/pets/dogyears");

    FieldValueConditionalReplacerConfig repl2 = new FieldValueConditionalReplacerConfig();
    repl2.operator = "EQUALS";
    repl2.comparisonValue = "Chip";
    repl2.replacementValue = "Butch";
    repl2.fieldNames = new ArrayList<>();
    repl2.fieldNames.add("/pets/cat");

    ArrayList<FieldValueConditionalReplacerConfig> toReplace = new ArrayList<>();
    toReplace.add(repl);
    toReplace.add(repl2);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs", null)
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", toReplace)
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, "Steve"));
      map.put("streetAddress", Field.create(Field.Type.STRING, "123 Street"));

      map.put("pets", Field.create(Field.Type.MAP,
          ImmutableMap.builder()
              .put("dog", Field.create(Field.Type.STRING, "Manny"))
              .put("dogyears", Field.create(Field.Type.INTEGER, 19))
              .put("cat", Field.create(Field.Type.STRING, "Chip"))
          .build()));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertTrue(result.size() == 3);

      Assert.assertTrue(result.containsKey("pets"));

      Field f2 = result.get("pets");
      Map<String, Field> res2 = f2.getValueAsMap();
      Assert.assertTrue(res2.containsKey("dog"));
      Assert.assertEquals("Manny", res2.get("dog").getValueAsString());

      Assert.assertTrue(res2.containsKey("dogyears"));
      Assert.assertEquals(3, res2.get("dogyears").getValueAsInteger());

      Assert.assertTrue(res2.containsKey("cat"));
      Assert.assertEquals("Butch", res2.get("cat").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConditionallyNullStringFieldsIfTheyAreEmpty() throws Exception {
    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/stringField");
    nullReplacerConditionalConfig.condition = "${str:length(record:value('/stringField')) == 0}";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", null)
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("stringField", Field.create(Field.Type.STRING, ""));

      Record record1 = RecordCreator.create("s", "s:1");
      record1.set(Field.create(map));

      map.clear();

      map.put("stringField", Field.create(Field.Type.STRING, "Steve"));

      Record record2 = RecordCreator.create("s", "s:2");
      record2.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2));
      List<Record> outputRecords = output.getRecords().get("a");

      Assert.assertEquals(2, outputRecords.size());

      Record outputRecord1 = outputRecords.get(0);

      Assert.assertTrue(outputRecord1.has("/stringField"));
      Assert.assertNull(outputRecord1.get("/stringField").getValueAsString());

      Record outputRecord2 = outputRecords.get(1);

      Assert.assertTrue(outputRecord2.has("/stringField"));
      Assert.assertNotNull(outputRecord2.get("/stringField").getValueAsString());
      Assert.assertEquals("Steve", outputRecord2.get("/stringField").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConditionallyNullNumericFieldsIfLessThanOrEqualToZero() throws Exception {
    NullReplacerConditionalConfig nullReplacerConditionalConfig1 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig1.fieldsToNull = ImmutableList.of("/shortField");
    nullReplacerConditionalConfig1.condition = "${record:value('/shortField') <= 0}";

    NullReplacerConditionalConfig nullReplacerConditionalConfig2 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig2.fieldsToNull = ImmutableList.of("/intField");
    nullReplacerConditionalConfig2.condition = "${record:value('/intField') <= 0}";

    NullReplacerConditionalConfig nullReplacerConditionalConfig3 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig3.fieldsToNull = ImmutableList.of("/longField");
    nullReplacerConditionalConfig3.condition = "${record:value('/longField') <= 0}";

    NullReplacerConditionalConfig nullReplacerConditionalConfig4 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig4.fieldsToNull = ImmutableList.of("/floatField");
    nullReplacerConditionalConfig4.condition = "${record:value('/floatField') <= 0}";

    NullReplacerConditionalConfig nullReplacerConditionalConfig5 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig5.fieldsToNull = ImmutableList.of("/doubleField");
    nullReplacerConditionalConfig5.condition = "${record:value('/doubleField') <= 0}";

    NullReplacerConditionalConfig nullReplacerConditionalConfig6 = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig6.fieldsToNull = ImmutableList.of("/decimalField");
    nullReplacerConditionalConfig6.condition = "${record:value('/decimalField') <= 0}";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs",
            ImmutableList.of(
                nullReplacerConditionalConfig1,
                nullReplacerConditionalConfig2,
                nullReplacerConditionalConfig3,
                nullReplacerConditionalConfig4,
                nullReplacerConditionalConfig5,
                nullReplacerConditionalConfig6
            )
        )
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", null)
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("shortField", Field.create(Field.Type.SHORT, 0));
      map.put("intField", Field.create(Field.Type.INTEGER, 0));
      map.put("longField", Field.create(Field.Type.LONG, 0L));
      map.put("floatField", Field.create(Field.Type.FLOAT, 0.0000F));
      map.put("doubleField", Field.create(Field.Type.DOUBLE, 0.0000D));
      map.put("decimalField", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(0).setScale(20, BigDecimal.ROUND_UNNECESSARY)));

      Record record1 = RecordCreator.create("s", "s:1");
      record1.set(Field.create(map));

      map.clear();
      map.put("shortField", Field.create(Field.Type.SHORT, -1));
      map.put("intField", Field.create(Field.Type.INTEGER, -2));
      map.put("longField", Field.create(Field.Type.LONG, -3L));
      map.put("floatField", Field.create(Field.Type.FLOAT, -4.05F));
      map.put("doubleField", Field.create(Field.Type.DOUBLE, -5.06D));
      map.put("decimalField", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(-6.07).setScale(20, BigDecimal.ROUND_DOWN)));

      Record record2 = RecordCreator.create("s", "s:2");
      record2.set(Field.create(map));

      map.clear();
      map.put("shortField", Field.create(Field.Type.SHORT, 1));
      map.put("intField", Field.create(Field.Type.INTEGER, 2));
      map.put("longField", Field.create(Field.Type.LONG, 3L));
      map.put("floatField", Field.create(Field.Type.FLOAT, 4.05F));
      map.put("doubleField", Field.create(Field.Type.DOUBLE, 5.06D));
      map.put("decimalField", Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(6.07).setScale(20, BigDecimal.ROUND_UP)));

      Record record3 = RecordCreator.create("s", "s:3");
      record3.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2, record3));
      List<Record> outputRecords = output.getRecords().get("a");

      Assert.assertEquals(3, outputRecords.size());

      Record outputRecord1 = outputRecords.get(0);

      Assert.assertNull(outputRecord1.get("/shortField").getValue());
      Assert.assertNull(outputRecord1.get("/intField").getValue());
      Assert.assertNull(outputRecord1.get("/longField").getValue());
      Assert.assertNull(outputRecord1.get("/floatField").getValue());
      Assert.assertNull(outputRecord1.get("/doubleField").getValue());
      Assert.assertNull(outputRecord1.get("/decimalField").getValue());

      Record outputRecord2 = outputRecords.get(1);

      Assert.assertNull(outputRecord2.get("/shortField").getValue());
      Assert.assertNull(outputRecord2.get("/intField").getValue());
      Assert.assertNull(outputRecord2.get("/longField").getValue());
      Assert.assertNull(outputRecord2.get("/floatField").getValue());
      Assert.assertNull(outputRecord2.get("/doubleField").getValue());
      Assert.assertNull(outputRecord2.get("/decimalField").getValue());

      Record outputRecord3 = outputRecords.get(2);

      Assert.assertEquals(1, outputRecord3.get("/shortField").getValueAsShort());
      Assert.assertEquals(2, outputRecord3.get("/intField").getValueAsInteger());
      Assert.assertEquals(3L, outputRecord3.get("/longField").getValue());
      Assert.assertEquals(4.05F, outputRecord3.get("/floatField").getValueAsFloat(), 0);
      Assert.assertEquals(5.06D, outputRecord3.get("/doubleField").getValueAsDouble(), 0);
      Assert.assertEquals(
          BigDecimal.valueOf(6.07).setScale(20, BigDecimal.ROUND_UP),
          outputRecord3.get("/decimalField").getValueAsDecimal()
      );

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConditionallyNullMultipleFieldsWithSingleCondition() throws Exception {
    NullReplacerConditionalConfig nullReplacerConditionalConfig = new NullReplacerConditionalConfig();
    nullReplacerConditionalConfig.fieldsToNull = ImmutableList.of("/b", "/c", "/d", "/e[*]", "/f/*");
    nullReplacerConditionalConfig.condition = "${record:value('/a') == NULL}";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs", ImmutableList.of(nullReplacerConditionalConfig))
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", null)
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a", Field.create(Field.Type.STRING, null));
      map.put("b", Field.create(Field.Type.INTEGER, 2));
      map.put("c", Field.create(Field.Type.SHORT, 3));
      map.put("d", Field.create(Field.Type.LONG, 4L));
      map.put("e", Field.create(Field.Type.LIST, ImmutableList.of(Field.create("list1_1"), Field.create("list1_2"), Field.create("list1_3"))));
      map.put("f", Field.create(Field.Type.LIST_MAP, new LinkedHashMap<>(ImmutableMap.of("map_a", Field.create("map_a1"), "map_b", Field.create("map_b1"), "map_c", Field.create("map_c1")))));


      Record record1 = RecordCreator.create("s", "s:1");
      record1.set(Field.create(map));

      map.clear();
      map.put("a", Field.create(Field.Type.STRING, "Not Null"));
      map.put("b", Field.create(Field.Type.INTEGER, 2));
      map.put("c", Field.create(Field.Type.SHORT, 3));
      map.put("d", Field.create(Field.Type.LONG, 4L));
      map.put("e", Field.create(Field.Type.LIST, ImmutableList.of(Field.create("list2_1"), Field.create("list2_2"), Field.create("list2_3"))));
      map.put("f", Field.create(Field.Type.LIST_MAP, new LinkedHashMap<>(ImmutableMap.of("map_a", Field.create("map2_1"), "map_b", Field.create("map2_2"), "map_c", Field.create("map2_3")))));

      Record record2 = RecordCreator.create("s", "s:2");
      record2.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2));
      List<Record> outputRecords = output.getRecords().get("a");

      Assert.assertEquals(2, outputRecords.size());

      Record outputRecord1 = outputRecords.get(0);

      Assert.assertNull(outputRecord1.get("/a").getValue());
      Assert.assertNull(outputRecord1.get("/b").getValue());
      Assert.assertNull(outputRecord1.get("/c").getValue());
      Assert.assertNull(outputRecord1.get("/d").getValue());

      Assert.assertNotNull(outputRecord1.get("/e").getValue());
      for (Field list_field : outputRecord1.get("/e").getValueAsList()) {
        Assert.assertNull(list_field.getValue());
      }

      Assert.assertNotNull(outputRecord1.get("/f").getValue());
      for (Field map_field : outputRecord1.get("/f").getValueAsMap().values()) {
        Assert.assertNull(map_field.getValue());
      }

      Record outputRecord2 = outputRecords.get(1);
      Assert.assertEquals("Not Null", outputRecord2.get("/a").getValueAsString());
      Assert.assertEquals(2, outputRecord2.get("/b").getValueAsInteger());
      Assert.assertEquals(3, outputRecord2.get("/c").getValueAsShort());
      Assert.assertEquals(4L, outputRecord2.get("/d").getValueAsLong());
      Assert.assertNotNull(outputRecord2.get("/e").getValue());
      int i = 1;
      for (Field list_field : outputRecord2.get("/e").getValueAsList()) {
        Assert.assertNotNull(list_field.getValue());
        Assert.assertEquals("list2_" + (i++), list_field.getValueAsString());
      }
      Assert.assertNotNull(outputRecord1.get("/f").getValue());
      i = 1;
      for (Field map_field : outputRecord2.get("/f").getValueAsMap().values()) {
        Assert.assertNotNull(map_field.getValue());
        Assert.assertEquals("map2_" + (i++), map_field.getValueAsString());

      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testPreserveExceptionCauseReplacementValue() throws StageException {

    FieldValueConditionalReplacerConfig replacement1 = new FieldValueConditionalReplacerConfig();
    replacement1.operator = "LESS_THAN";
    replacement1.comparisonValue = "40.0";
    replacement1.replacementValue = "invalidNumber";
    replacement1.fieldNames = new ArrayList<>();
    replacement1.fieldNames.add("/stateTax");
    replacement1.fieldNames.add("/fedTax");

    assertNumberFormatExceptionCause(replacement1);
  }

  @Test
  public void testPreserveExceptionCauseComparisonValue() throws StageException {
    FieldValueConditionalReplacerConfig replacement1 = new FieldValueConditionalReplacerConfig();
    replacement1.operator = "LESS_THAN";
    replacement1.comparisonValue = "stillInvalidNumber";
    replacement1.replacementValue = "25.0";
    replacement1.fieldNames = new ArrayList<>();
    replacement1.fieldNames.add("/stateTax");
    replacement1.fieldNames.add("/fedTax");

    assertNumberFormatExceptionCause(replacement1);
  }

  public static void assertNumberFormatExceptionCause(FieldValueConditionalReplacerConfig config) throws StageException {

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerDProcessor.class)
        .addConfiguration("nullReplacerConditionalConfigs", null)
        .addConfiguration("fieldsToReplaceIfNull", null)
        .addConfiguration("fieldsToConditionallyReplace", Collections.singletonList(config))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addOutputLane("a")
        .build();
    runner.runInit();

    try {
      Record record = getConditionallyReplaceRecord();

      try {
        StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
        Assert.fail("Exception should have been thrown");
      } catch (Exception e) {
        // IllegalArgumentException is the type we throw
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause(), notNullValue());
        // in the case of exception during comparison, there is another wrapped IllegalArgumentException
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
      }
    } finally {
      runner.runDestroy();
    }
  }
}
