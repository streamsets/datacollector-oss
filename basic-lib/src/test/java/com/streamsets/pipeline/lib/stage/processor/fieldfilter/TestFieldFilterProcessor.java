/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldfilter;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldFilterProcessor {

  @Test
  public void testKeep() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name", "/age"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 2);
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertTrue(result.containsKey("age"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testKeepNonExistingFiled() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/city"))
      .addConfiguration("filterOperation", FilterOperation.KEEP)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 0);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemove() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/name", "/age"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
      map.put("streetAddress", Field.create("c"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("streetAddress"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRemoveNonExistingFiled() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFilterProcessor.class)
      .addConfiguration("fields", ImmutableList.of("/city"))
      .addConfiguration("filterOperation", FilterOperation.REMOVE)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create("a"));
      map.put("age", Field.create("b"));
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
      Assert.assertTrue(result.containsKey("age"));
      Assert.assertTrue(result.containsKey("streetAddress"));
    } finally {
      runner.runDestroy();
    }
  }

}
