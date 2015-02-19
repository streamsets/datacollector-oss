/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldhasher;

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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestFieldHasherProcessor {

  @Test
  public void testStringField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/name");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("streamsets", HashType.SHA2), result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDecimalField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DECIMAL, new BigDecimal(345.678)));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new BigDecimal(345.678).toString(), HashType.SHA2),
        result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIntegerField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.INTEGER, -123));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("-123", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testShortField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.SHORT, 123));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("123", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLongField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.LONG, 21474836478L));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("21474836478", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BYTE, 125));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("125", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCharField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.CHAR, 'c'));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("c", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testBooleanField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BOOLEAN, true));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("true", HashType.SHA2), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATE, new Date(123456789)));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new Date(123456789).toString(), HashType.SHA2),
        result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDateTimeField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATETIME, new Date(123456789)));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new Date(123456789).toString(), HashType.SHA2),
        result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFloatField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.FLOAT, 2.3842e-07f));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(String.valueOf(2.3842e-07f), HashType.SHA2),
        result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDoubleField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DOUBLE, 12342.3842));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(String.valueOf(12342.3842), HashType.SHA2),
        result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testByteArrayField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/byteArray");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("byteArray", Field.create(Field.Type.BYTE_ARRAY, "streamsets".getBytes()));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("byteArray"));
      Assert.assertEquals(computeHash("streamsets", HashType.SHA2),
        result.get("byteArray").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleFields() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/age", "/name", "/sex", "/streetAddress");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, null));
      map.put("streetAddress", Field.create("c"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("a", HashType.SHA2), result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("21", HashType.SHA2), result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash("c", HashType.SHA2), result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMultipleFieldsDifferentHashTypes() throws StageException {
    FieldHasherConfig sha1HasherConfig = new FieldHasherConfig();
    sha1HasherConfig.fieldsToHash = ImmutableList.of("/age", "/name");
    sha1HasherConfig.hashType = HashType.SHA1;

    FieldHasherConfig sha2HasherConfig = new FieldHasherConfig();
    sha2HasherConfig.fieldsToHash = ImmutableList.of("/sex");
    sha2HasherConfig.hashType = HashType.SHA2;

    FieldHasherConfig md5HasherConfig = new FieldHasherConfig();
    md5HasherConfig.fieldsToHash = ImmutableList.of("/streetAddress");
    md5HasherConfig.hashType = HashType.MD5;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(sha1HasherConfig, sha2HasherConfig, md5HasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, "male"));
      map.put("streetAddress", Field.create("sansome street"));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("a", HashType.SHA1), result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("21", HashType.SHA1), result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(computeHash("male", HashType.SHA2), result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash("sansome street", HashType.MD5),
        result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingField() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/nonExisting");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
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
  public void testUnsupportedFieldTypes() throws StageException {
    FieldHasherConfig fieldHasherConfig = new FieldHasherConfig();
    fieldHasherConfig.fieldsToHash = ImmutableList.of("/name", "/mapField", "/listField");
    fieldHasherConfig.hashType = HashType.SHA2;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fieldHasherConfigs", ImmutableList.of(fieldHasherConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));

      Map<String, Field> mapField = new HashMap<>();
      mapField.put("e1", Field.create("e1"));
      mapField.put("e2", Field.create("e2"));

      map.put("mapField", Field.create(mapField));
      map.put("listField", Field.create(Field.Type.LIST, ImmutableList.of(Field.create("e1"), Field.create("e2"))));

            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("streamsets", HashType.SHA2), result.get("name").getValue());

      Assert.assertTrue(result.containsKey("mapField"));
      Map<String, Field> m =  result.get("mapField").getValueAsMap();
      Assert.assertEquals("e1", m.get("e1").getValueAsString());
      Assert.assertEquals("e2", m.get("e2").getValueAsString());

      Assert.assertTrue(result.containsKey("listField"));
      List<Field> l = result.get("listField").getValueAsList();
      Assert.assertEquals("e1", l.get(0).getValueAsString());
      Assert.assertEquals("e2", l.get(1).getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  private String computeHash(String obj, HashType hashType) {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance(hashType.getDigest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    messageDigest.update(obj.getBytes());
    byte byteData[] = messageDigest.digest();

    //encode byte[] into hex
    StringBuilder sb = new StringBuilder();
    for(byte b : byteData) {
      sb.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString();
  }
}
