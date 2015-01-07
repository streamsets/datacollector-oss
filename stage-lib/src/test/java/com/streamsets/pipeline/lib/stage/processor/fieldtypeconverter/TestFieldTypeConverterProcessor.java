/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldTypeConverterProcessor {

  @Test
  public void testStringToBoolean() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.BOOLEAN;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("false"));
      map.put("intermediate", Field.create("yes"));
      map.put("advanced", Field.create("no"));
      map.put("expert", Field.create("true"));
      map.put("skilled", Field.create("122345566"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(false, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(false, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(false, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(true, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(false, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToByte() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.BYTE;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("1"));
      map.put("intermediate", Field.create("126"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals((byte)1, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals((byte)126, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToChar() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.CHAR;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("a"));
      map.put("intermediate", Field.create("yes"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals('a', result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals('y', result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToByteArray() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.BYTE_ARRAY;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("abc"));
      map.put("intermediate", Field.create("yes"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertTrue(Arrays.equals("abc".getBytes(), (byte[])result.get("beginner").getValue()));
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertTrue(Arrays.equals("yes".getBytes(), (byte[]) result.get("intermediate").getValue()));
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDecimalEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.DECIMAL;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("expert", Field.create("1.234"));
      map.put("advanced", Field.create("1,234"));
      map.put("beginner", Field.create("1,234.56789"));
      map.put("intermediate", Field.create("1.234,56789"));
      map.put("skilled", Field.create("-1.23E-12"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(BigDecimal.valueOf(1234.56789), result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(BigDecimal.valueOf(1.234), result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
      Assert.assertEquals(BigDecimal.valueOf(1.234), result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(BigDecimal.valueOf(1234), result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(BigDecimal.valueOf(-1.23E-12), result.get("skilled").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDecimalGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.DECIMAL;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("expert", Field.create("1234.5678"));
      map.put("advanced", Field.create("1234"));
      map.put("beginner", Field.create("1,234.56789"));
      map.put("intermediate", Field.create("1.234,56789"));
      map.put("skilled", Field.create("-1.23E-12"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(BigDecimal.valueOf(1.234), result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(BigDecimal.valueOf(1234.56789), result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(BigDecimal.valueOf(12345678), result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(BigDecimal.valueOf(1234), result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      //FIXME<Hari>: Is this ok?
      Assert.assertEquals(BigDecimal.valueOf(-1.23E-10), result.get("skilled").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDoubleEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.DOUBLE;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("expert", Field.create("1.234"));
      map.put("advanced", Field.create("1,234"));
      map.put("beginner", Field.create("1,234.56789"));
      map.put("intermediate", Field.create("1.234,56789"));
      map.put("skilled", Field.create("-1.23E-12"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(1234.56789, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(1.234, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
      Assert.assertEquals(1.234, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals((double)1234, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(-1.23E-12, result.get("skilled").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDoubleGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.DECIMAL;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("expert", Field.create("1234.5678"));
      map.put("advanced", Field.create("1234"));
      map.put("beginner", Field.create("1,234.56789"));
      map.put("intermediate", Field.create("1.234,56789"));
      map.put("skilled", Field.create("-1.23E-12"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(BigDecimal.valueOf(1.234), result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(BigDecimal.valueOf(1234.56789), result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(BigDecimal.valueOf(12345678), result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(BigDecimal.valueOf(1234), result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      //FIXME<Hari>: Is this ok?
      Assert.assertEquals(BigDecimal.valueOf(-1.23E-10), result.get("skilled").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToIntegerEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.INTEGER;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("-12234"));
      map.put("intermediate", Field.create("-1,234"));
      map.put("advanced", Field.create("12,567"));
      map.put("expert", Field.create("1.234"));
      map.put("skilled", Field.create("1234"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(-12234, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(-1234, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(12567, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(1, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(1234, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToIntegerGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.INTEGER;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("-12234"));
      map.put("intermediate", Field.create("-1,234"));
      map.put("advanced", Field.create("12,567"));
      map.put("expert", Field.create("1.234"));
      map.put("skilled", Field.create("1234"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(-12234, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(-1, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(12, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(1234, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(1234, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToLongEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.LONG;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("12345678910"));
      map.put("intermediate", Field.create("10L"));
      map.put("advanced", Field.create("-10L"));
      map.put("expert", Field.create("12,345,678,910"));
      map.put("skilled", Field.create("12.345.678.910"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(12345678910L, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(10L, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(12L, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(12345678910L, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(-10L, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToLongGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.LONG;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("12345678910"));
      map.put("intermediate", Field.create("10L"));
      map.put("advanced", Field.create("-10L"));
      map.put("expert", Field.create("12,345,678,910"));
      map.put("skilled", Field.create("12.345.678.910"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(12345678910L, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals(10L, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(12345678910L, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(12L, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(-10L, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToShortEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.SHORT;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("32768"));
      map.put("intermediate", Field.create("10"));
      map.put("advanced", Field.create("42768"));
      map.put("expert", Field.create("-32,767"));
      map.put("skilled", Field.create("32.767"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals((short)32768, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals((short)10, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals((short)32, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals((short)-32767, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals((short)-22768, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToShortGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.SHORT;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("32768"));
      map.put("intermediate", Field.create("10"));
      map.put("advanced", Field.create("42768"));
      map.put("expert", Field.create("-32,767"));
      map.put("skilled", Field.create("32.767"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals((short)32768, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
      Assert.assertEquals((short)10, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals((short)32767, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals((short)-32, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals((short)-22768, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToFloatEnglishLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.FLOAT;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("10.4f"));
      map.put("intermediate", Field.create("2.3842e-07f"));
      map.put("advanced", Field.create("3.14157"));
      map.put("expert", Field.create("-3,767.45"));
      map.put("skilled", Field.create("3.767,45"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(10.4f, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
//      Assert.assertEquals(2.3842e-07f, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(3.767f, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(-3767.45f, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(3.14157f, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToFloatGermanLocale() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert", "/null");
    fieldTypeConverterConfig.targetType = FieldTypeConverterProcessor.FieldType.FLOAT;
    fieldTypeConverterConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.GERMAN;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("10.4f"));
      map.put("intermediate", Field.create("2.3842e-07f"));
      map.put("advanced", Field.create("3.14157"));
      map.put("expert", Field.create("-3,767.45"));
      map.put("skilled", Field.create("3.767,45"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);
      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(104f, result.get("beginner").getValue());
      Assert.assertTrue(result.containsKey("intermediate"));
//      Assert.assertEquals(2.3842e-07f, result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(3767.45f, result.get("skilled").getValue());
      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(-3.767f, result.get("expert").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(314157f, result.get("advanced").getValue());
      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(null, result.get("null").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDate() throws StageException {
    FieldTypeConverterProcessor.FieldTypeConverterConfig beginnerConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    beginnerConfig.fields = ImmutableList.of("/beginner");
    beginnerConfig.targetType = FieldTypeConverterProcessor.FieldType.DATE;
    beginnerConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;
    beginnerConfig.dateFormat = "yyyy-MM-dd";

    FieldTypeConverterProcessor.FieldTypeConverterConfig intermediateConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    intermediateConfig.fields = ImmutableList.of("/intermediate");
    intermediateConfig.targetType = FieldTypeConverterProcessor.FieldType.DATE;
    intermediateConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;
    intermediateConfig.dateFormat = "dd-MM-YYYY";

    FieldTypeConverterProcessor.FieldTypeConverterConfig skilledConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    skilledConfig.fields = ImmutableList.of("/skilled");
    skilledConfig.targetType = FieldTypeConverterProcessor.FieldType.DATE;
    skilledConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;
    skilledConfig.dateFormat = "yyyy-MM-dd HH:mm:ss";

    FieldTypeConverterProcessor.FieldTypeConverterConfig advancedConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    advancedConfig.fields = ImmutableList.of("/advanced");
    advancedConfig.targetType = FieldTypeConverterProcessor.FieldType.DATE;
    advancedConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;
    advancedConfig.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    FieldTypeConverterProcessor.FieldTypeConverterConfig expertConfig =
      new FieldTypeConverterProcessor.FieldTypeConverterConfig();
    expertConfig.fields = ImmutableList.of("/expert");
    expertConfig.targetType = FieldTypeConverterProcessor.FieldType.DATE;
    expertConfig.dataLocale = FieldTypeConverterProcessor.DataLocale.ENGLISH;
    expertConfig.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS Z";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(beginnerConfig, advancedConfig,
        intermediateConfig, skilledConfig, expertConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("2015-01-03")); //
      map.put("intermediate", Field.create("03-01-2015"));
      map.put("advanced", Field.create("2015-01-03 21:31:02.777"));//
      map.put("expert", Field.create("2015-01-03 21:32:32.333 PST"));//
      map.put("skilled", Field.create("2015-01-03 21:30:01"));//
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);

      Assert.assertTrue(result.containsKey("beginner"));
      SimpleDateFormat beginnerDateFormat = new SimpleDateFormat(beginnerConfig.dateFormat);
      Assert.assertEquals("2015-01-03", beginnerDateFormat.format(result.get("beginner").getValueAsDate()));

      /*Assert.assertTrue(result.containsKey("intermediate"));
      SimpleDateFormat intermediateDateFormat = new SimpleDateFormat(intermediateConfig.dateFormat);
      Assert.assertEquals("03-01-2015", intermediateDateFormat.format(result.get("intermediate").getValueAsDate()));*/

      Assert.assertTrue(result.containsKey("advanced"));
      SimpleDateFormat advancedDateFormat = new SimpleDateFormat(advancedConfig.dateFormat);
      Assert.assertEquals("2015-01-03 21:31:02.777",
        advancedDateFormat.format(result.get("advanced").getValueAsDate()));

      Assert.assertTrue(result.containsKey("expert"));
      SimpleDateFormat expertDateFormat = new SimpleDateFormat(expertConfig.dateFormat);
      Assert.assertEquals("2015-01-03 21:32:32.333 -0800",
        expertDateFormat.format(result.get("expert").getValueAsDate()));

      Assert.assertTrue(result.containsKey("skilled"));
      SimpleDateFormat skilledDateFormat = new SimpleDateFormat(skilledConfig.dateFormat);
      Assert.assertEquals("2015-01-03 21:30:01", skilledDateFormat.format(result.get("skilled").getValueAsDate()));

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(null, result.get("null").getValueAsDate());

    } finally {
      runner.runDestroy();
    }
  }

}
