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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DateFormat;
import com.streamsets.pipeline.config.DecimalScaleRoundingStrategy;
import com.streamsets.pipeline.config.ZonedDateTimeFormat;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static com.streamsets.pipeline.stage.processor.fieldtypeconverter.Errors.CONVERTER_04;

public class TestFieldTypeConverterProcessorFields {

  @Test
  public void testStringToNonExistentField() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
        new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/nonExistent", "/beginner", "/expert", "/skilled");
    fieldTypeConverterConfig.targetType = Field.Type.BOOLEAN;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
      Assert.assertTrue(Arrays.equals("abc".getBytes(), (byte[]) result.get("beginner").getValue()));
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
    fieldTypeConverterConfig.scale = -1;
    fieldTypeConverterConfig.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UNNECESSARY;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
      Assert.assertEquals("9", result.get("beginner").getAttribute(HeaderAttributeConstants.ATTR_PRECISION));
      Assert.assertEquals("5", result.get("beginner").getAttribute(HeaderAttributeConstants.ATTR_SCALE));
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
    fieldTypeConverterConfig.scale = -1;
    fieldTypeConverterConfig.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UNNECESSARY;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
      Assert.assertEquals((double) 1234, result.get("advanced").getValue());
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
    fieldTypeConverterConfig.scale = -1;
    fieldTypeConverterConfig.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UNNECESSARY;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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

  public void testStringToDateTimeTypes(Field.Type type) throws Exception {
    FieldTypeConverterConfig beginnerConfig =
        new FieldTypeConverterConfig();
    beginnerConfig.fields = ImmutableList.of("/beginner");
    beginnerConfig.targetType = type;
    beginnerConfig.dataLocale = "en";
    beginnerConfig.dateFormat = DateFormat.OTHER;
    beginnerConfig.otherDateFormat ="yyyy-MM-dd";

    FieldTypeConverterConfig intermediateConfig =
        new FieldTypeConverterConfig();
    intermediateConfig.fields = ImmutableList.of("/intermediate");
    intermediateConfig.targetType = type;
    intermediateConfig.dataLocale = "en";
    intermediateConfig.dateFormat = DateFormat.OTHER;
    intermediateConfig.otherDateFormat = "dd-MM-YYYY";

    FieldTypeConverterConfig skilledConfig =
        new FieldTypeConverterConfig();
    skilledConfig.fields = ImmutableList.of("/skilled");
    skilledConfig.targetType = type;
    skilledConfig.dataLocale = "en";
    skilledConfig.dateFormat = DateFormat.OTHER;
    skilledConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss";

    FieldTypeConverterConfig advancedConfig =
        new FieldTypeConverterConfig();
    advancedConfig.fields = ImmutableList.of("/advanced");
    advancedConfig.targetType = type;
    advancedConfig.dataLocale = "en";
    advancedConfig.dateFormat = DateFormat.OTHER;
    advancedConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    FieldTypeConverterConfig expertConfig =
        new FieldTypeConverterConfig();
    expertConfig.fields = ImmutableList.of("/expert");
    expertConfig.targetType = type;
    expertConfig.dataLocale = "en";
    expertConfig.dateFormat = DateFormat.OTHER;
    expertConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss.SSS Z";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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

      Assert.assertTrue(result.containsKey("advanced"));
      Assert.assertEquals(type,result.get("advanced").getType());
      SimpleDateFormat advancedDateFormat = new SimpleDateFormat(advancedConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:31:02.777",
          advancedDateFormat.format(result.get("advanced").getValueAsDate()));

      Assert.assertTrue(result.containsKey("expert"));
      Assert.assertEquals(type,result.get("expert").getType());
      SimpleDateFormat expertDateFormat = new SimpleDateFormat(expertConfig.otherDateFormat);
      SimpleDateFormat expertDataSourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
      Assert.assertEquals(expertDateFormat.format(expertDataSourceFormat.parse("2015-01-03 21:32:32.333 PST")),
          expertDateFormat.format(result.get("expert").getValueAsDate()));

      Assert.assertTrue(result.containsKey("skilled"));
      Assert.assertEquals(type,result.get("skilled").getType());
      SimpleDateFormat skilledDateFormat = new SimpleDateFormat(skilledConfig.otherDateFormat);
      Assert.assertEquals("2015-01-03 21:30:01", skilledDateFormat.format(result.get("skilled").getValueAsDate()));

      Assert.assertTrue(result.containsKey("null"));
      Assert.assertEquals(Field.Type.STRING, result.get("null").getType());
      Assert.assertEquals(null, result.get("null").getValueAsDate());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToDateTime() throws Exception {
    testStringToDateTimeTypes(Field.Type.DATETIME);
  }

  @Test
  public void testStringToDate() throws Exception {
    testStringToDateTimeTypes(Field.Type.DATE);
  }

  @Test
  public void testStringToTime() throws Exception {
    testStringToDateTimeTypes(Field.Type.TIME);
  }

  @Test
  public void testLongToDateTimeString() throws Exception {
    FieldTypeConverterConfig dtConfig =
        new FieldTypeConverterConfig();
    dtConfig.fields = ImmutableList.of("/dateString");
    dtConfig.targetType = Field.Type.STRING;
    dtConfig.dataLocale = "en";
    dtConfig.treatInputFieldAsDate = true;
    dtConfig.dateFormat = DateFormat.OTHER;
    dtConfig.otherDateFormat = "yyyy-MM-dd";

    FieldTypeConverterConfig stringConfig =
        new FieldTypeConverterConfig();
    stringConfig.fields = ImmutableList.of("/LongString");
    stringConfig.targetType = Field.Type.STRING;
    stringConfig.treatInputFieldAsDate = false;
    stringConfig.dataLocale = "en";
    stringConfig.dateFormat = DateFormat.OTHER;
    stringConfig.otherDateFormat = "yyyy-MM-dd HH:mm:ss";


    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(stringConfig, dtConfig))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("LongString", Field.create(1431763314761L));
      map.put("dateString", Field.create(1431763314761L));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals("1431763314761", result.get("LongString").getValueAsString());
      Assert.assertEquals("2015-05-16", result.get("dateString").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateTimeToLong() throws Exception {
    FieldTypeConverterConfig dtConfig =
        new FieldTypeConverterConfig();
    dtConfig.fields = ImmutableList.of("/dateTime");
    dtConfig.targetType = Field.Type.LONG;
    dtConfig.dataLocale = "en";

    FieldTypeConverterConfig dateConfig =
        new FieldTypeConverterConfig();
    dateConfig.fields = ImmutableList.of("/date");
    dateConfig.targetType = Field.Type.LONG;
    dateConfig.dataLocale = "en";


    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(dateConfig, dtConfig))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("date", Field.createDate(new Date(0)));
      map.put("dateTime", Field.createDatetime(new Date(0)));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 0, result.get("date").getValueAsLong());
      Assert.assertEquals(String.valueOf(result), 0, result.get("dateTime").getValueAsLong());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateTimeToString() throws Exception {
    FieldTypeConverterConfig dtConfig =
        new FieldTypeConverterConfig();
    dtConfig.fields = ImmutableList.of("/dateTime");
    dtConfig.targetType = Field.Type.STRING;
    dtConfig.dataLocale = "en";
    dtConfig.dateFormat = DateFormat.DD_MM_YYYY;

    FieldTypeConverterConfig dateConfig =
        new FieldTypeConverterConfig();
    dateConfig.fields = ImmutableList.of("/date");
    dateConfig.targetType = Field.Type.STRING;
    dateConfig.dataLocale = "en";
    dateConfig.dateFormat = DateFormat.OTHER;
    dateConfig.otherDateFormat = "yyyy-MM-dd";


    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(dateConfig, dtConfig))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.DAY_OF_MONTH, 20);
      cal.set(Calendar.MONTH, 1);
      cal.set(Calendar.YEAR, 2015);

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("date", Field.createDate(cal.getTime()));
      map.put("dateTime", Field.createDatetime(cal.getTime()));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals("20-Feb-2015", result.get("dateTime").getValue());
      Assert.assertEquals("2015-02-20", result.get("date").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testZonedDatetimeToString() throws Exception {
    FieldTypeConverterConfig config1 =
        new FieldTypeConverterConfig();
    config1.fields = ImmutableList.of("/zdt-1");
    config1.targetType = Field.Type.STRING;
    config1.dataLocale = "en";
    config1.zonedDateTimeFormat = ZonedDateTimeFormat.ISO_ZONED_DATE_TIME;

    FieldTypeConverterConfig config2 =
        new FieldTypeConverterConfig();
    config2.fields = ImmutableList.of("/zdt-2");
    config2.targetType = Field.Type.STRING;
    config2.dataLocale = "en";
    config2.zonedDateTimeFormat = ZonedDateTimeFormat.ISO_OFFSET_DATE_TIME;


    FieldTypeConverterConfig config3 =
        new FieldTypeConverterConfig();
    config3.fields = ImmutableList.of("/zdt-3");
    config3.targetType = Field.Type.STRING;
    config3.dataLocale = "en";
    config3.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config3.otherZonedDateTimeFormat = "YYYY-MM-ddX";

    FieldTypeConverterConfig config4 =
        new FieldTypeConverterConfig();
    config4.fields = ImmutableList.of("/zdt-4");
    config4.targetType = Field.Type.STRING;
    config4.dataLocale = "en";
    config4.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config4.otherZonedDateTimeFormat = "YYYY-MM-dd'T'HH:mm:ssX[VV]";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
                                 .addConfiguration("convertBy", ConvertBy.BY_FIELD)
                                 .addConfiguration("fieldTypeConverterConfigs",
                                     ImmutableList.of(config1, config2, config3, config4))
                                 .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    ZonedDateTime current = ZonedDateTime.now();
    map.put("zdt-1", Field.createZonedDateTime(current));
    map.put("zdt-2", Field.createZonedDateTime(current));
    map.put("zdt-3", Field.createZonedDateTime(current));
    map.put("zdt-4", Field.createZonedDateTime(current));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      Record output = runner.runProcess(ImmutableList.of(record)).getRecords().get("a").get(0);
      Assert.assertEquals(current.format(DateTimeFormatter.ISO_ZONED_DATE_TIME),
          output.get("/zdt-1").getValueAsString());
      Assert.assertEquals(current.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
          output.get("/zdt-2").getValueAsString());
      Assert.assertEquals(current.format(DateTimeFormatter.ofPattern("YYYY-MM-ddX")),
          output.get("/zdt-3").getValueAsString());
      Assert.assertEquals(current.format(DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ssX[VV]")),
          output.get("/zdt-4").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStringToZonedDatetime() throws Exception {
    ZonedDateTime current = ZonedDateTime.now();
    FieldTypeConverterConfig config1 =
        new FieldTypeConverterConfig();
    config1.fields = ImmutableList.of("/zdt-1");
    config1.targetType = Field.Type.ZONED_DATETIME;
    config1.dataLocale = "en";
    config1.zonedDateTimeFormat = ZonedDateTimeFormat.ISO_ZONED_DATE_TIME;

    FieldTypeConverterConfig config2 =
        new FieldTypeConverterConfig();
    config2.fields = ImmutableList.of("/zdt-2");
    config2.targetType = Field.Type.ZONED_DATETIME;
    config2.dataLocale = "en";
    config2.zonedDateTimeFormat = ZonedDateTimeFormat.ISO_OFFSET_DATE_TIME;

    FieldTypeConverterConfig config3 =
        new FieldTypeConverterConfig();
    config3.fields = ImmutableList.of("/zdt-3");
    config3.targetType = Field.Type.ZONED_DATETIME;
    config3.dataLocale = "en";
    config3.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config3.otherZonedDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX'['VV']'";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
                                 .addConfiguration("convertBy", ConvertBy.BY_FIELD)
                                 .addConfiguration("fieldTypeConverterConfigs",
                                     ImmutableList.of(config1, config2, config3))
                                 .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("zdt-1", Field.create(ZonedDateTimeFormat.ISO_ZONED_DATE_TIME.getFormatter().get().format(current)));
    map.put("zdt-2", Field.create(ZonedDateTimeFormat.ISO_OFFSET_DATE_TIME.getFormatter().get().format(current)));
    map.put("zdt-3", Field.create(
        current.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX'['VV']'", Locale.ENGLISH))));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      Record output = runner.runProcess(ImmutableList.of(record)).getRecords().get("a").get(0);
      Assert.assertEquals(current, output.get("/zdt-1").getValueAsZonedDateTime());
      Assert.assertEquals(current.toOffsetDateTime().toZonedDateTime(), output.get("/zdt-2").getValueAsZonedDateTime());
      Assert.assertEquals(current, output.get("/zdt-3").getValueAsZonedDateTime());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidType() throws Exception {
    FieldTypeConverterConfig config1 =
        new FieldTypeConverterConfig();
    config1.fields = ImmutableList.of("/zdt-1");
    config1.targetType = Field.Type.LONG;
    config1.dataLocale = "en";
    config1.zonedDateTimeFormat = ZonedDateTimeFormat.ISO_ZONED_DATE_TIME;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs",
            ImmutableList.of(config1))
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    ZonedDateTime current = ZonedDateTime.now();
    map.put("zdt-1", Field.createZonedDateTime(current));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      runner.runProcess(ImmutableList.of(record));
      Assert.fail();
    } catch (StageException ex) {
      Assert.assertEquals(CONVERTER_04.getCode(), ex.getErrorCode().getCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidMask() {

    List<FieldTypeConverterConfig> valids = new ArrayList<>();
    List<FieldTypeConverterConfig> invalids = new ArrayList<>();

    FieldTypeConverterConfig config1 =
        new FieldTypeConverterConfig();
    config1.fields = ImmutableList.of("/zdt-1");
    config1.targetType = Field.Type.ZONED_DATETIME;
    config1.dataLocale = "en";
    config1.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config1.otherZonedDateTimeFormat = "yyyy-MM-ddX"; // Invalid

    invalids.add(config1);

    FieldTypeConverterConfig config2 =
        new FieldTypeConverterConfig();
    config2.fields = ImmutableList.of("/zdt-2");
    config2.targetType = Field.Type.ZONED_DATETIME;
    config2.dataLocale = "en";
    config2.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config2.otherZonedDateTimeFormat = "yyyy-MM'T'HHX"; // Invalid

    invalids.add(config2);

    FieldTypeConverterConfig config3 =
        new FieldTypeConverterConfig();
    config3.fields = ImmutableList.of("/zdt-3");
    config3.targetType = Field.Type.ZONED_DATETIME;
    config3.dataLocale = "en";
    config3.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config3.otherZonedDateTimeFormat = "yyyy-MM-dd'T'HHX"; // Valid

    valids.add(config3);

    FieldTypeConverterConfig config4 =
        new FieldTypeConverterConfig();
    config4.fields = ImmutableList.of("/zdt-3");
    config4.targetType = Field.Type.ZONED_DATETIME;
    config4.dataLocale = "en";
    config4.zonedDateTimeFormat = ZonedDateTimeFormat.OTHER;
    config4.otherZonedDateTimeFormat =  "yyyy-MM-dd'T'HH:mm:ss.SSSX"; // Valid

    valids.add(config4);

    invalids.forEach(config -> {
      try {
        ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
                                     .addConfiguration("convertBy", ConvertBy.BY_FIELD)
                                     .addConfiguration("fieldTypeConverterConfigs",
                                         ImmutableList.of(config))
                                     .addOutputLane("a").build();
        runner.runInit();
        Assert.fail("Expected StageException for Field: "  + config.fields.get(0));
      } catch (StageException ignored) {
      }
    });

    valids.forEach(config -> {
      try {
        ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
                                     .addConfiguration("convertBy", ConvertBy.BY_FIELD)
                                     .addConfiguration("fieldTypeConverterConfigs",
                                         ImmutableList.of(config))
                                     .addOutputLane("a").build();
        runner.runInit();
      } catch (StageException ex) {
        Assert.fail("Unexpected StageException for field, " + config.fields.get(0) + ": " +
                        Throwables.getStackTraceAsString(ex));
      }
    });
  }


  @Test
  public void testInvalidConversionFieldsNumber() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig =
        new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = ImmutableList.of("/invalidConversion");
    fieldTypeConverterConfig.targetType = Field.Type.FLOAT;
    fieldTypeConverterConfig.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("invalidConversion", Field.create("float"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      runner.runProcess(ImmutableList.of(record));
      Assert.fail("Expected OnRecordException");
    } catch(OnRecordErrorException e) {
      //No-op
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("invalidConversion", Field.create("Hello World"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      runner.runProcess(ImmutableList.of(record));
      Assert.fail("Expected OnRecordException");
    } catch(OnRecordErrorException e) {
      //No-op
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(fieldTypeConverterConfig))
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("invalidConversion", Field.create(1.0));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      runner.runProcess(ImmutableList.of(record));
      Assert.fail("Expected OnRecordException");
    } catch(OnRecordErrorException e) {
      //No-op
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
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

  @Test
  public void testDateConversion() throws StageException {

    FieldTypeConverterConfig date7 = new FieldTypeConverterConfig();
    date7.fields = ImmutableList.of("/date7");
    date7.targetType = Field.Type.DATE;
    date7.dateFormat = DateFormat.YYYY_MM_DD_T_HH_MM_SS_SSS_Z;
    date7.dataLocale = "en";

    FieldTypeConverterConfig date6 = new FieldTypeConverterConfig();
    date6.fields = ImmutableList.of("/date6");
    date6.targetType = Field.Type.DATE;
    date6.dateFormat = DateFormat.YYYY_MM_DD_T_HH_MM_Z;
    date6.dataLocale = "en";

    FieldTypeConverterConfig date5 = new FieldTypeConverterConfig();
    date5.fields = ImmutableList.of("/date5");
    date5.targetType = Field.Type.DATE;
    date5.dateFormat = DateFormat.YYYY_MM_DD_HH_MM_SS_SSS_Z;
    date5.dataLocale = "en";

    FieldTypeConverterConfig date4 = new FieldTypeConverterConfig();
    date4.fields = ImmutableList.of("/date4");
    date4.targetType = Field.Type.DATE;
    date4.dateFormat = DateFormat.YYYY_MM_DD_HH_MM_SS_SSS;
    date4.dataLocale = "en";

    FieldTypeConverterConfig date3 = new FieldTypeConverterConfig();
    date3.fields = ImmutableList.of("/date3");
    date3.targetType = Field.Type.DATE;
    date3.dateFormat = DateFormat.YYYY_MM_DD_HH_MM_SS;
    date3.dataLocale = "en";

    FieldTypeConverterConfig date2 = new FieldTypeConverterConfig();
    date2.fields = ImmutableList.of("/date2");
    date2.targetType = Field.Type.DATE;
    date2.dateFormat = DateFormat.DD_MM_YYYY;
    date2.dataLocale = "en";

    FieldTypeConverterConfig date1 = new FieldTypeConverterConfig();
    date1.fields = ImmutableList.of("/date1");
    date1.targetType = Field.Type.DATE;
    date1.dateFormat = DateFormat.YYYY_MM_DD;
    date1.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(date1, date2, date3, date4, date5, date6, date7))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("date1", Field.create(Field.Type.STRING, "2016-08-12"));
      map.put("date2", Field.create(Field.Type.STRING, "12-AUG-2016"));
      map.put("date3", Field.create(Field.Type.STRING, "2016-08-12 16:08:12"));
      map.put("date4", Field.create(Field.Type.STRING, "2016-08-12 16:08:12.777"));
      map.put("date5", Field.create(Field.Type.STRING, "2016-08-12 16:08:12.777 -0700"));
      map.put("date6", Field.create(Field.Type.STRING, "2015-08-11T18:32Z"));
      map.put("date7", Field.create(Field.Type.STRING, "2016-08-15T18:32:12.777Z"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 7);

      Assert.assertTrue(result.containsKey("date1"));
      Assert.assertEquals(Field.Type.DATE, result.get("date1").getType());
      SimpleDateFormat date1DateFormat = new SimpleDateFormat(date1.dateFormat.getFormat());
      Assert.assertEquals("2016-08-12", date1DateFormat.format(result.get("date1").getValue()));

      Assert.assertTrue(result.containsKey("date2"));
      Assert.assertEquals(Field.Type.DATE, result.get("date2").getType());
      SimpleDateFormat date2DateFormat = new SimpleDateFormat(date2.dateFormat.getFormat());
      Assert.assertEquals("12-Aug-2016", date2DateFormat.format(result.get("date2").getValue()));

      Assert.assertTrue(result.containsKey("date3"));
      Assert.assertEquals(Field.Type.DATE, result.get("date3").getType());
      SimpleDateFormat date3DateFormat = new SimpleDateFormat(date3.dateFormat.getFormat());
      Assert.assertEquals("2016-08-12 16:08:12", date3DateFormat.format(result.get("date3").getValue()));

      Assert.assertTrue(result.containsKey("date4"));
      Assert.assertEquals(Field.Type.DATE, result.get("date4").getType());
      SimpleDateFormat date4DateFormat = new SimpleDateFormat(date4.dateFormat.getFormat());
      Assert.assertEquals("2016-08-12 16:08:12.777", date4DateFormat.format(result.get("date4").getValue()));

      Assert.assertTrue(result.containsKey("date5"));
      Assert.assertEquals(Field.Type.DATE, result.get("date5").getType());
      SimpleDateFormat date5DateFormat = new SimpleDateFormat(date5.dateFormat.getFormat());
      date5DateFormat.setTimeZone(TimeZone.getTimeZone("MST"));
      Assert.assertEquals("2016-08-12 16:08:12.777 -0700", date5DateFormat.format(result.get("date5").getValue()));

      Assert.assertTrue(result.containsKey("date6"));
      Assert.assertEquals(Field.Type.DATE, result.get("date6").getType());
      SimpleDateFormat date6DateFormat = new SimpleDateFormat(date6.dateFormat.getFormat());
      Assert.assertEquals("2015-08-11T18:32Z", date6DateFormat.format(result.get("date6").getValue()));

      Assert.assertTrue(result.containsKey("date7"));
      Assert.assertEquals(Field.Type.DATE, result.get("date7").getType());
      SimpleDateFormat date7DateFormat = new SimpleDateFormat(date7.dateFormat.getFormat());
      Assert.assertEquals("2016-08-15T18:32:12.777Z", date7DateFormat.format(result.get("date7").getValue()));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteArrayToString() throws StageException {
    FieldTypeConverterConfig converter1 =
        new FieldTypeConverterConfig();
    converter1.fields = ImmutableList.of("/byteUTF8");
    converter1.encoding = "UTF-8";
    converter1.targetType = Field.Type.STRING;
    converter1.dataLocale = "en";

    FieldTypeConverterConfig converter2 =
        new FieldTypeConverterConfig();
    converter2.fields = ImmutableList.of("/byteGBK");
    converter2.encoding = "GBK";
    converter2.targetType = Field.Type.STRING;
    converter2.dataLocale = "en";

    FieldTypeConverterConfig converter3 =
        new FieldTypeConverterConfig();
    converter3.fields = ImmutableList.of("/badEncoding");
    converter3.encoding = "GBK";
    converter3.targetType = Field.Type.STRING;
    converter3.dataLocale = "en";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(converter1, converter2, converter3))
        .addOutputLane("a").build();
    runner.runInit();

    final String someUTF8 = "UTF8: euro: € greek όλα τα ελληνικά σε μένα japanese 天気の良い日 thai วันที่ดี data.";
    final byte [] byteUTF8 = someUTF8.getBytes(Charset.forName("UTF8"));

    try {
      final String x = "GBK: japanese 天気の良い日 trad chinese 傳統 simplified 镕 chinese 中国是美丽的 data.";
      String someGBK = new String(x.getBytes("UTF8"), "GBK");
      final byte[] byteGBK = x.getBytes();

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteUTF8", Field.create(Field.Type.BYTE_ARRAY, byteUTF8));
      map.put("byteGBK", Field.create(Field.Type.BYTE_ARRAY, byteGBK));
      map.put("badEncoding", Field.create(Field.Type.BYTE_ARRAY, byteUTF8));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();

      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("byteUTF8"));
      Assert.assertEquals(someUTF8, result.get("byteUTF8").getValueAsString());

      Assert.assertTrue(result.containsKey("byteGBK"));
      Assert.assertEquals(someGBK, result.get("byteGBK").getValueAsString());

      Assert.assertTrue(result.containsKey("badEncoding"));
      // this is UTF8 data decoded as GBK - which does not work.
      Assert.assertNotEquals(someGBK, result.get("badEncoding").getValueAsString());
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConvertToDecimalUnsupportedTypes() throws StageException {
    FieldTypeConverterConfig converter = new FieldTypeConverterConfig();
    converter.fields = ImmutableList.of("/");
    converter.encoding = "UTF-8";
    converter.targetType = Field.Type.DECIMAL;
    converter.scale = -1;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(converter))
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    //MAP
    Record record1 = RecordCreator.create();
    record1.set(Field.create(new HashMap<String, Field>()));

    //List_MAP
    Record record2 = RecordCreator.create();
    record2.set(Field.createListMap(new LinkedHashMap<String, Field>()));

    //LIST
    Record record3 = RecordCreator.create();
    record3.set(Field.create(new ArrayList<Field>()));

    //Byte Array
    Record record4 = RecordCreator.create();
    record4.set(Field.create("Sample".getBytes()));

    //Boolean
    Record record5 = RecordCreator.create();
    record5.set(Field.create(Boolean.TRUE));

    //char
    Record record6 = RecordCreator.create();
    record6.set(Field.create('a'));

    //File Ref
    Record record7 = RecordCreator.create();
    record7.set(Field.create(Field.Type.FILE_REF, new FileRef(1000) {
      @Override
      @SuppressWarnings("unchecked")
      public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
        return ImmutableSet.of((Class<T>)InputStream.class);
      }

      @Override
      @SuppressWarnings("unchecked")
      public <T extends AutoCloseable> T createInputStream(ProtoConfigurableEntity.Context context, Class<T> streamClassType) throws IOException {
        return (T) new ByteArrayInputStream("Sample".getBytes());
      }
    }));

    //Date
    Record record8 = RecordCreator.create();
    record8.set(Field.create(Field.Type.DATE, new Date()));

    //Date
    Record record9 = RecordCreator.create();
    record9.set(Field.create(Field.Type.DATETIME, new Date()));

    //Time
    Record record10 = RecordCreator.create();
    record10.set(Field.create(Field.Type.TIME, new Date()));


    List<Record> records =
        Arrays.asList(record1, record2, record3, record4, record5, record6, record7, record8, record9, record10);

    try {
      StageRunner.Output output = runner.runProcess(records);
      List<Record> outputRecords = output.getRecords().get("a");
      List<Record> errorRecords = runner.getErrorRecords();

      Assert.assertEquals(0, outputRecords.size());
      Assert.assertEquals(records.size(), errorRecords.size());

      for (Record record : errorRecords) {
        Assert.assertEquals(Errors.CONVERTER_00.name(), record.getHeader().getErrorCode());
      }

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConvertToDecimalSupportedTypesWithoutScale() throws Exception {
    Map<String, ?> bigDecimalMap =
        new ImmutableMap.Builder<String, Object>()
            .put("/byte", (byte)'a')
            .put("/short1", Short.MAX_VALUE)
            .put("/short2", Short.MIN_VALUE)
            .put("/int1", Integer.MIN_VALUE)
            .put("/int2", Integer.MAX_VALUE)
            .put("/float1", Float.MIN_VALUE)
            .put("/float2", Float.MAX_VALUE)
            .put("/long1", Long.MIN_VALUE)
            .put("/long2", Long.MAX_VALUE)
            .put("/double1", Double.MIN_VALUE)
            .put("/double2", Double.MAX_VALUE)
            .put("/decimal1", BigDecimal.ZERO)
            .put("/decimal2", BigDecimal.ONE)
            .put("/decimal3", BigDecimal.TEN)
            .put("/string1", new BigDecimal("123.456").setScale(3, BigDecimal.ROUND_UNNECESSARY))
            .build();

    FieldTypeConverterConfig converter = new FieldTypeConverterConfig();
    converter.fields = new ArrayList<>(bigDecimalMap.keySet());
    converter.dataLocale = "UTF-8";
    converter.targetType = Field.Type.DECIMAL;
    converter.scale = -1;
    converter.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UP;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration("fieldTypeConverterConfigs", ImmutableList.of(converter))
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byte", Field.create(Field.Type.BYTE, bigDecimalMap.get("/byte")));

      map.put("short1", Field.create(Field.Type.SHORT, bigDecimalMap.get("/short1")));
      map.put("short2", Field.create(Field.Type.SHORT, bigDecimalMap.get("/short2")));

      map.put("int1", Field.create(Field.Type.INTEGER, bigDecimalMap.get("/int1")));
      map.put("int2", Field.create(Field.Type.INTEGER, bigDecimalMap.get("/int2")));

      map.put("float1", Field.create(Field.Type.FLOAT, bigDecimalMap.get("/float1")));
      map.put("float2", Field.create(Field.Type.FLOAT, bigDecimalMap.get("/float2")));

      map.put("long1", Field.create(Field.Type.LONG, bigDecimalMap.get("/long1")));
      map.put("long2", Field.create(Field.Type.LONG, bigDecimalMap.get("/long2")));

      map.put("double1", Field.create(Field.Type.DOUBLE, bigDecimalMap.get("/double1")));
      map.put("double2", Field.create(Field.Type.DOUBLE, bigDecimalMap.get("/double2")));

      map.put("decimal1", Field.create(Field.Type.DOUBLE, bigDecimalMap.get("/decimal1")));
      map.put("decimal2", Field.create(Field.Type.DOUBLE, bigDecimalMap.get("/decimal2")));
      map.put("decimal3", Field.create(Field.Type.DOUBLE, bigDecimalMap.get("/decimal3")));

      map.put("string1", Field.create(Field.Type.STRING, "123.456"));


      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Map<String, Field> fieldMap = output.getRecords().get("a").get(0).get().getValueAsMap();

      for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
        Field field = entry.getValue();
        Assert.assertEquals(Field.Type.DECIMAL, field.getType());
        Assert.assertEquals(new BigDecimal(field.getValueAsString()), field.getValueAsDecimal());
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testConvertDecimalWithDifferentScaleAndRoundingStrategy() throws Exception {
    FieldTypeConverterConfig converter1 = new FieldTypeConverterConfig();

    converter1.fields = ImmutableList.of("/decimal1", "/decimal2");
    converter1.targetType = Field.Type.DECIMAL;
    converter1.scale = 6;
    converter1.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UP;

    FieldTypeConverterConfig converter2 = new FieldTypeConverterConfig();
    converter2.fields = ImmutableList.of("/decimal3", "/decimal4");
    converter2.targetType = Field.Type.DECIMAL;
    converter2.scale = 4;
    converter2.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_DOWN;

    FieldTypeConverterConfig converter3 = new FieldTypeConverterConfig();
    converter3.fields = ImmutableList.of("/decimal5", "/decimal6");
    converter3.targetType = Field.Type.DECIMAL;
    converter3.scale = 3;
    converter3.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_CEILING;

    FieldTypeConverterConfig converter4 = new FieldTypeConverterConfig();
    converter4.fields = ImmutableList.of("/decimal7", "/decimal8");
    converter4.targetType = Field.Type.DECIMAL;
    converter4.scale = 2;
    converter4.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_FLOOR;


    FieldTypeConverterConfig converter5 = new FieldTypeConverterConfig();
    converter5.fields = ImmutableList.of("/decimal9", "/decimal10");
    converter5.targetType = Field.Type.DECIMAL;
    converter5.scale = 5;
    converter5.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_HALF_UP;

    FieldTypeConverterConfig converter6 = new FieldTypeConverterConfig();
    converter6.fields = ImmutableList.of("/decimal11", "/decimal12");
    converter6.targetType = Field.Type.DECIMAL;
    converter6.scale = 5;
    converter6.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_HALF_DOWN;

    FieldTypeConverterConfig converter7 = new FieldTypeConverterConfig();
    converter7.fields = ImmutableList.of("/decimal13");
    converter7.targetType = Field.Type.DECIMAL;
    converter7.scale = 4;
    converter7.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_HALF_EVEN;


    FieldTypeConverterConfig converter8 = new FieldTypeConverterConfig();
    converter8.fields = ImmutableList.of("/int");
    converter8.targetType = Field.Type.DECIMAL;
    converter8.scale = 5;
    converter8.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UNNECESSARY;

    FieldTypeConverterConfig converter9 = new FieldTypeConverterConfig();
    converter9.fields = ImmutableList.of("/double1");
    converter9.targetType = Field.Type.DECIMAL;
    converter9.scale = 10;
    converter9.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_FLOOR;

    FieldTypeConverterConfig converter10 = new FieldTypeConverterConfig();
    converter10.fields = ImmutableList.of("/double2");
    converter10.targetType = Field.Type.DECIMAL;
    converter10.scale = 1;
    converter10.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_CEILING;

    FieldTypeConverterConfig converter11 = new FieldTypeConverterConfig();
    converter11.fields = ImmutableList.of("/string1", "/string2");
    converter11.targetType = Field.Type.DECIMAL;
    converter11.scale = 6;
    converter11.dataLocale = "UTF-8";
    converter11.decimalScaleRoundingStrategy = DecimalScaleRoundingStrategy.ROUND_UP;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
        .addConfiguration("convertBy", ConvertBy.BY_FIELD)
        .addConfiguration(
            "fieldTypeConverterConfigs",
            Arrays.asList(
                converter1,
                converter2,
                converter3,
                converter4,
                converter5,
                converter6,
                converter7,
                converter8,
                converter9,
                converter10,
                converter11
            )
        )
        .addOutputLane("a").build();
    runner.runInit();
    try {
      Map<String, Field> fieldMap = new HashMap<>();

      //round up
      fieldMap.put("decimal1", Field.create(new BigDecimal("12345.6789115")));
      fieldMap.put("decimal2", Field.create(new BigDecimal("-12345.6789115")));

      //round down
      fieldMap.put("decimal3", Field.create(new BigDecimal("12345.67895")));
      fieldMap.put("decimal4", Field.create(new BigDecimal("-12345.67895")));

      //round ceil
      fieldMap.put("decimal5", Field.create(new BigDecimal("12345.67115")));
      fieldMap.put("decimal6", Field.create(new BigDecimal("-12345.67115")));

      //round floor
      fieldMap.put("decimal7", Field.create(new BigDecimal("12345.67891")));
      fieldMap.put("decimal8", Field.create(new BigDecimal("-12345.67891")));

      //round half up
      fieldMap.put("decimal9", Field.create(new BigDecimal("12345.678945")));
      fieldMap.put("decimal10", Field.create(new BigDecimal("-12345.678945")));

      //round half down
      fieldMap.put("decimal11", Field.create(new BigDecimal("12345.678945")));
      fieldMap.put("decimal12", Field.create(new BigDecimal("-12345.678945")));

      //round half even
      fieldMap.put("decimal13", Field.create(new BigDecimal("12345.678750")));

      //int - no rounding necessary
      fieldMap.put("int", Field.create(Field.Type.INTEGER, 1));
      //double - round floor
      fieldMap.put("double1", Field.create(Field.Type.DOUBLE, 1234567.98765));
      //double - round ceiling
      fieldMap.put("double2", Field.create(Field.Type.DOUBLE, 1234567.98765));

      //string - round up
      fieldMap.put("string1", Field.create(Field.Type.STRING, "12345.6789115"));
      fieldMap.put("string2", Field.create(Field.Type.STRING, "-12345.6789115"));


      Record record = RecordCreator.create();
      record.set(Field.create(fieldMap));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());

      Record outputRecord = output.getRecords().get("a").get(0);

      //round up
      Assert.assertEquals(new BigDecimal("12345.678912"), outputRecord.get("/decimal1").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.678912"), outputRecord.get("/decimal2").getValueAsDecimal());

      //round down
      Assert.assertEquals(new BigDecimal("12345.6789"), outputRecord.get("/decimal3").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.6789"), outputRecord.get("/decimal4").getValueAsDecimal());

      //round ceil
      Assert.assertEquals(new BigDecimal("12345.672"), outputRecord.get("/decimal5").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.671"), outputRecord.get("/decimal6").getValueAsDecimal());

      //round floor
      Assert.assertEquals(new BigDecimal("12345.67"), outputRecord.get("/decimal7").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.68"), outputRecord.get("/decimal8").getValueAsDecimal());

      //half up
      Assert.assertEquals(new BigDecimal("12345.67895"), outputRecord.get("/decimal9").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.67895"), outputRecord.get("/decimal10").getValueAsDecimal());

      //half down
      Assert.assertEquals(new BigDecimal("12345.67894"), outputRecord.get("/decimal11").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.67894"), outputRecord.get("/decimal12").getValueAsDecimal());

      //half even
      Assert.assertEquals(new BigDecimal("12345.6788"), outputRecord.get("/decimal13").getValueAsDecimal());

      //int with rounding unnecessary and double with different rounding
      Assert.assertEquals(new BigDecimal("1.00000"), outputRecord.get("/int").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("1234567.9876500000"), outputRecord.get("/double1").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("1234568.0"), outputRecord.get("/double2").getValueAsDecimal());

      //string round up
      Assert.assertEquals(new BigDecimal("12345.678912"), outputRecord.get("/string1").getValueAsDecimal());
      Assert.assertEquals(new BigDecimal("-12345.678912"), outputRecord.get("/string2").getValueAsDecimal());
    } finally {
      runner.runDestroy();
    }
  }

}
