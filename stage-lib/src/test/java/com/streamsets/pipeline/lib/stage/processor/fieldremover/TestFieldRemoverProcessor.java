/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldremover;

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

public class TestFieldRemoverProcessor {

  @Test
  public void testFieldRemoverProcessor() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRemoverProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/age", "/streetAddress"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldRemoverProcessorNonExistingFiled() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRemoverProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/city"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create("c"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 3);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("a", result.get("name").getValue());
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertEquals(21, result.get("age").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals("c", result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldRemoverProcessorMultipleFields() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRemoverProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/streetAddress", "/age"))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create("c"));
      Record record = new RecordImpl("s", "s:1", null, null);
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals("a", result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }
}
