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
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldHasherProcessor {

  @Test
  public void testFieldHasherProcessor() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name", "/age", "/sex", "/streetAddress"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("sex", Field.create(Field.Type.STRING, null));
      map.put("streetAddress", Field.create("c"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("a"), result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("21"), result.get("age").getValue());
      Assert.assertTrue(result.containsKey("sex"));
      Assert.assertEquals(null, result.get("sex").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals(computeHash("c"), result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherString() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsets"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(computeHash("streamsets"), result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherBigDecimal() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DECIMAL, new BigDecimal(345.678)));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new BigDecimal(345.678).toString()), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherInteger() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.INTEGER, -123));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("-123"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherShort() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.SHORT, 123));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("123"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherLong() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.LONG, 21474836478L));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("21474836478"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherByte() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BYTE, 125));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("125"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherChar() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.CHAR, 'c'));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("c"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherBoolean() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.BOOLEAN, true));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash("true"), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherDate() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATE, new Date(123456789)));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new Date(123456789).toString()), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherDateTime() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.DATETIME, new Date(123456789)));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(new Date(123456789).toString()), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldHasherFloat() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldHasherProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create(Field.Type.FLOAT, 2.3842e-07f));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(computeHash(String.valueOf(2.3842e-07f)), result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  private String computeHash(String obj) {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
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
