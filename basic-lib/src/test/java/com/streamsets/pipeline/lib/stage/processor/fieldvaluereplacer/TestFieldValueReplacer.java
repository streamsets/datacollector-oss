/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldvaluereplacer;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldValueReplacer {

  @Test
  public void testNonExistingFields() throws StageException {

    FieldValueReplacerConfig nameReplacement = new FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/nonExisting");
    nameReplacement.newValue = "StreamSets";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/nonExisting"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
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
  public void testNullConfiguration() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", null)
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
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", new ArrayList<>())
      .addConfiguration("fieldsToReplaceIfNull", new ArrayList<>())
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
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", null)
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(stringFieldReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement, addressReplacement))
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

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacerProcessor.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement))
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
}
