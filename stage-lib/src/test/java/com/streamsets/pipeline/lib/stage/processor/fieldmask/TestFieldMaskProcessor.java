/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldmask;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldMaskProcessor {

  @Test
  public void testFieldMaskProcessorVariableLength() throws StageException {

    FieldMaskProcessor.FieldMaskConfig nameMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    nameMaskConfig.maskType = "VARIABLE_LENGTH_MASK";
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxxxxxxxxxx", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFixedLength() throws StageException {

    FieldMaskProcessor.FieldMaskConfig ageMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    ageMaskConfig.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    ageMaskConfig.maskType = "FIXED_LENGTH_MASK";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(ageMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xxxxxxxxxx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxxxxxxxxx", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFormatPreserving() throws StageException {

    FieldMaskProcessor.FieldMaskConfig formatPreserveMask = new FieldMaskProcessor.FieldMaskConfig();
    formatPreserveMask.fields = ImmutableList.of("/name", "/age", "/ssn", "/phone");
    formatPreserveMask.maskType = "xxx-xx-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(formatPreserveMask))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxx-xx-mset", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxx-xx-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals(null, result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorNonString() throws StageException {

    FieldMaskProcessor.FieldMaskConfig formatPreserveMask = new FieldMaskProcessor.FieldMaskConfig();
    formatPreserveMask.fields = ImmutableList.of("/name", "/age");
    formatPreserveMask.maskType = "xxx-xx-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(formatPreserveMask))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(12345));
      map.put("age", Field.create(123.56));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 2);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(12345, result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(123.56, result.get("age").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorMultipleFormats() throws StageException {

    FieldMaskProcessor.FieldMaskConfig nameMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name");
    nameMaskConfig.maskType = "VARIABLE_LENGTH_MASK";

    FieldMaskProcessor.FieldMaskConfig ageMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    ageMaskConfig.fields = ImmutableList.of("/age");
    ageMaskConfig.maskType = "FIXED_LENGTH_MASK";

    FieldMaskProcessor.FieldMaskConfig ssnMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    ssnMaskConfig.fields = ImmutableList.of("/ssn");
    ssnMaskConfig.maskType = "xxx-xx-####";

    FieldMaskProcessor.FieldMaskConfig phoneMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    phoneMaskConfig.fields = ImmutableList.of("/phone");
    phoneMaskConfig.maskType = "###-###-####";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig, ageMaskConfig, ssnMaskConfig, phoneMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("streamsetsinc"));
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create("9876543210"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 4);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("xxxxxxxxxxxxx", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("xxxxxxxxxx", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("xxx-xx-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals("987-654-3210", result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldMaskProcessorFiledDoesNotExist() throws StageException {

    FieldMaskProcessor.FieldMaskConfig nameMaskConfig = new FieldMaskProcessor.FieldMaskConfig();
    nameMaskConfig.fields = ImmutableList.of("/name");
    nameMaskConfig.maskType = "VARIABLE_LENGTH_MASK";

ProcessorRunner runner = new ProcessorRunner.Builder(FieldMaskProcessor.class)
      .addConfiguration("fieldMaskConfigs", ImmutableList.of(nameMaskConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("age", Field.create("12"));
      map.put("ssn", Field.create("123-45-6789"));
      map.put("phone", Field.create("9876543210"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals("12", result.get("age").getValue());
      Assert.assertTrue(result.containsKey("ssn"));
      Assert.assertEquals("123-45-6789", result.get("ssn").getValue());
      Assert.assertTrue(result.containsKey("phone"));
      Assert.assertEquals("9876543210", result.get("phone").getValue());
    } finally {
      runner.runDestroy();
    }
  }
}
