/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DateFormat;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldTypeConverterProcessor {

  @Test
  public void testStringToNonExistentField() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/nonExistent", "/beginner", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = Field.Type.BOOLEAN;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
      Assert.assertEquals("yes", result.get("intermediate").getValue());
      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals("no", result.get("advanced").getValue());
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
  public void testStringToBoolean() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced", "/expert"
      , "/null");
    fieldTypeConverterConfig.targetType = Field.Type.BOOLEAN;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.BYTE;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("1"));
      map.put("intermediate", Field.create("126"));
      map.put("null", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.CHAR;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("a"));
      map.put("intermediate", Field.create("yes"));
      map.put("null", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.BYTE_ARRAY;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("beginner", Field.create("abc"));
      map.put("intermediate", Field.create("yes"));
      map.put("null", Field.create(Field.Type.STRING, null));
      Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert",
      "/skilled", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.DECIMAL;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert",
      "/skilled", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.DECIMAL;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert",
      "/skilled", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.DOUBLE;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/advanced", "/expert",
      "/skilled", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.DECIMAL;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.INTEGER;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.INTEGER;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.LONG;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.LONG;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.SHORT;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.SHORT;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.FLOAT;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/beginner", "/intermediate", "/skilled", "/advanced",
      "/expert", "/null");
    fieldTypeConverterConfig.targetType = Field.Type.FLOAT;
    fieldTypeConverterConfig.dataLocale = "de";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
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
  public void testStringToDate() throws Exception {
    FieldTypeConverterConfig beginnerConfig =
      new FieldTypeConverterConfig();
    beginnerConfig.fields = ImmutableList.of("/beginner");
    beginnerConfig.targetType = Field.Type.DATE;
    beginnerConfig.dataLocale = "en";
    beginnerConfig.dateFormat = DateFormat.OTHER;
    beginnerConfig.otherDateFormat = "yyyy-MM-dd";

    FieldTypeConverterConfig intermediateConfig =
      new FieldTypeConverterConfig();
    intermediateConfig.fields = ImmutableList.of("/intermediate");
    intermediateConfig.targetType = Field.Type.DATE;
    intermediateConfig.dataLocale = "en";
    intermediateConfig.dateFormat = DateFormat.OTHER;
    intermediateConfig.otherDateFormat = "dd-MM-YYYY";

    FieldTypeConverterConfig skilledConfig =
      new FieldTypeConverterConfig();
    skilledConfig.fields = ImmutableList.of("/skilled");
    skilledConfig.targetType = Field.Type.DATE;
    skilledConfig.dataLocale = "en";
    skilledConfig.dateFormat = DateFormat.OTHER;
    skilledConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss";

    FieldTypeConverterConfig advancedConfig =
      new FieldTypeConverterConfig();
    advancedConfig.fields = ImmutableList.of("/advanced");
    advancedConfig.targetType = Field.Type.DATE;
    advancedConfig.dataLocale = "en";
    advancedConfig.dateFormat = DateFormat.OTHER;
    advancedConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    FieldTypeConverterConfig expertConfig =
      new FieldTypeConverterConfig();
    expertConfig.fields = ImmutableList.of("/expert");
    expertConfig.targetType = Field.Type.DATE;
    expertConfig.dataLocale = "en";
    expertConfig.dateFormat = DateFormat.OTHER;
    expertConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS Z";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);

      Assert.assertTrue(result.containsKey("beginner"));
      Assert.assertEquals(Field.Type.DATE,result.get("beginner").getType());
      SimpleDateFormat beginnerDateFormat = new SimpleDateFormat(beginnerConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03", beginnerDateFormat.format(result.get("beginner").getValueAsDate()));

      /*Assert.assertTrue(result.containsKey("intermediate"));
      SimpleDateFormat intermediateDateFormat = new SimpleDateFormat(intermediateConfig.dateFormat);
      Assert.assertEquals("03-01-2015", intermediateDateFormat.format(result.get("intermediate").getValueAsDate()));*/

      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(Field.Type.DATE,result.get("advanced").getType());
      SimpleDateFormat advancedDateFormat = new SimpleDateFormat(advancedConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:31:02.777",
        advancedDateFormat.format(result.get("advanced").getValueAsDate()));

      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(Field.Type.DATE,result.get("expert").getType());
      SimpleDateFormat expertDateFormat = new SimpleDateFormat(expertConfig.otherDateFormat);
      SimpleDateFormat expertDataSourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
      Assert.assertEquals(expertDateFormat.format(expertDataSourceFormat.parse("2015-01-03 21:32:32.333 PST")),
        expertDateFormat.format(result.get("expert").getValueAsDate()));

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(Field.Type.DATE,result.get("skilled").getType());
      SimpleDateFormat skilledDateFormat = new SimpleDateFormat(skilledConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:30:01", skilledDateFormat.format(result.get("skilled").getValueAsDate()));

      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(Field.Type.STRING,result.get("null").getType());
      Assert.assertEquals(null, result.get("null").getValueAsDate());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDateTime() throws Exception {
    FieldTypeConverterConfig beginnerConfig =
      new FieldTypeConverterConfig();
    beginnerConfig.fields = ImmutableList.of("/beginner");
    beginnerConfig.targetType = Field.Type.DATETIME;
    beginnerConfig.dataLocale = "en";
    beginnerConfig.dateFormat = DateFormat.OTHER;
    beginnerConfig.otherDateFormat ="yyyy-MM-dd";

    FieldTypeConverterConfig intermediateConfig =
      new FieldTypeConverterConfig();
    intermediateConfig.fields = ImmutableList.of("/intermediate");
    intermediateConfig.targetType = Field.Type.DATETIME;
    intermediateConfig.dataLocale = "en";
    intermediateConfig.dateFormat = DateFormat.OTHER;
    intermediateConfig.otherDateFormat = "dd-MM-YYYY";

    FieldTypeConverterConfig skilledConfig =
      new FieldTypeConverterConfig();
    skilledConfig.fields = ImmutableList.of("/skilled");
    skilledConfig.targetType = Field.Type.DATETIME;
    skilledConfig.dataLocale = "en";
    skilledConfig.dateFormat = DateFormat.OTHER;
    skilledConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss";

    FieldTypeConverterConfig advancedConfig =
      new FieldTypeConverterConfig();
    advancedConfig.fields = ImmutableList.of("/advanced");
    advancedConfig.targetType = Field.Type.DATETIME;
    advancedConfig.dataLocale = "en";
    advancedConfig.dateFormat = DateFormat.OTHER;
    advancedConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    FieldTypeConverterConfig expertConfig =
      new FieldTypeConverterConfig();
    expertConfig.fields = ImmutableList.of("/expert");
    expertConfig.targetType = Field.Type.DATETIME;
    expertConfig.dataLocale = "en";
    expertConfig.dateFormat = DateFormat.OTHER;
    expertConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS Z";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
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
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 6);

      Assert.assertTrue(result.containsKey("beginner"));
      SimpleDateFormat beginnerDateFormat = new SimpleDateFormat(beginnerConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03", beginnerDateFormat.format(result.get("beginner").getValueAsDate()));

      /*Assert.assertTrue(result.containsKey("intermediate"));
      SimpleDateFormat intermediateDateFormat = new SimpleDateFormat(intermediateConfig.dateFormat);
      Assert.assertEquals("03-01-2015", intermediateDateFormat.format(result.get("intermediate").getValueAsDate()));*/

      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(Field.Type.DATETIME,result.get("advanced").getType());
      SimpleDateFormat advancedDateFormat = new SimpleDateFormat(advancedConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:31:02.777",
        advancedDateFormat.format(result.get("advanced").getValueAsDate()));

      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(Field.Type.DATETIME,result.get("expert").getType());
      SimpleDateFormat expertDateFormat = new SimpleDateFormat(expertConfig.otherDateFormat);
      SimpleDateFormat expertDataSourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
      Assert.assertEquals(expertDateFormat.format(expertDataSourceFormat.parse("2015-01-03 21:32:32.333 PST")),
        expertDateFormat.format(result.get("expert").getValueAsDate()));

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(Field.Type.DATETIME,result.get("skilled").getType());
      SimpleDateFormat skilledDateFormat = new SimpleDateFormat(skilledConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:30:01", skilledDateFormat.format(result.get("skilled").getValueAsDate()));

      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(Field.Type.STRING,result.get("null").getType());
      Assert.assertEquals(null, result.get("null").getValueAsDate());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidConversionFieldsNumber() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/invalidConversion");
    fieldTypeConverterConfig.targetType = Field.Type.FLOAT;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("invalidConversion", Field.create("float"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(0, output.getRecords().get("a").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidConversionStringToDate() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/invalidConversion");
    fieldTypeConverterConfig.targetType = Field.Type.DATE;
    fieldTypeConverterConfig.dataLocale = "en";
    fieldTypeConverterConfig.dateFormat = DateFormat.DD_MM_YYYY;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("invalidConversion", Field.create("Hello World"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      //Expect no output, record sent to error
      Assert.assertEquals(0, output.getRecords().get("a").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidConversionNonStringToDate() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
      new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/invalidConversion");
    fieldTypeConverterConfig.targetType = Field.Type.DATE;
    fieldTypeConverterConfig.dataLocale = "en";
    fieldTypeConverterConfig.dateFormat = DateFormat.DD_MM_YYYY;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("invalidConversion", Field.create(1.0));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      //Expect no output, record sent to error
      Assert.assertEquals(0, output.getRecords().get("a").size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonStringFieldToNonStringField() throws StageException {
    FieldTypeConverterConfig decimalToInteger =
      new FieldTypeConverterConfig();
    decimalToInteger.fields = ImmutableList.of("/base");
    decimalToInteger.targetType = Field.Type.INTEGER;

    FieldTypeConverterConfig floatToShort =
      new FieldTypeConverterConfig();
    floatToShort.fields = ImmutableList.of("/bonus");
    floatToShort.targetType = Field.Type.SHORT;

    FieldTypeConverterConfig longToByte =
      new FieldTypeConverterConfig();
    longToByte.fields = ImmutableList.of("/benefits");
    longToByte.targetType = Field.Type.BYTE;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(decimalToInteger, floatToShort, longToByte))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("base", Field.create(Field.Type.DECIMAL, new BigDecimal(1234.56)));
      map.put("bonus", Field.create(Field.Type.FLOAT, 200.45f));
      map.put("benefits", Field.create(Field.Type.LONG, 123456789L));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);

      Assert.assertTrue(result.containsKey("base"));
      Assert.assertEquals(Field.Type.INTEGER, result.get("base").getType());
      Assert.assertEquals(1234, result.get("base").getValue());

      Assert.assertTrue(result.containsKey("bonus"));
      Assert.assertEquals(Field.Type.SHORT, result.get("bonus").getType());
      Assert.assertEquals((short)200, result.get("bonus").getValue());

      Assert.assertTrue(result.containsKey("benefits"));
      Assert.assertEquals(Field.Type.BYTE, result.get("benefits").getType());
      Assert.assertEquals((byte)21, result.get("benefits").getValue());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWildCardConverter() throws StageException {

    Field name1 = Field.create("1");
    Field name2 = Field.create("2");
    Map<String, Field> nameMap1 = new HashMap<>();
    nameMap1.put("name", name1);
    Map<String, Field> nameMap2 = new HashMap<>();
    nameMap2.put("name", name2);

    Field name3 = Field.create("3");
    Field name4 = Field.create("4");
    Map<String, Field> nameMap3 = new HashMap<>();
    nameMap3.put("name", name3);
    Map<String, Field> nameMap4 = new HashMap<>();
    nameMap4.put("name", name4);

    Field name5 = Field.create("5");
    Field name6 = Field.create("6");
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

    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsString(), "1");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsString(), "2");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsString(), "3");
    Assert.assertEquals(record.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsString(), "4");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "5");
    Assert.assertEquals(record.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "6");

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
    FieldTypeConverterConfig stringToInt = new FieldTypeConverterConfig();
    stringToInt.fields = ImmutableList.of("/USA[*]/SanFrancisco/*/streets[*][*]/name");
    stringToInt.targetType = Field.Type.INTEGER;
    stringToInt.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(stringToInt))
      .addOutputLane("a").build();
    runner.runInit();

    try {

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record resultRecord = output.getRecords().get("a").get(0);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][0]/name").getValueAsInteger(), 1);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[0][1]/name").getValueAsInteger(), 2);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][0]/name").getValueAsInteger(), 3);
      Assert.assertEquals(resultRecord.get("/USA[0]/SanFrancisco/noe/streets[1][1]/name").getValueAsInteger(), 4);

      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][0]/name").getValueAsString(), "5");
      Assert.assertEquals(resultRecord.get("/USA[1]/SantaMonica/cole/streets[0][1]/name").getValueAsString(), "6");

    } finally {
      runner.runDestroy();
    }
  }
}
