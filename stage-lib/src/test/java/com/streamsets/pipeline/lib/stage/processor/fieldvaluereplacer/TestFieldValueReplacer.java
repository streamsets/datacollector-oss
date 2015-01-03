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
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldValueReplacer {

  @Test
  public void testFieldValueReplacerFieldsToNull() throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacer.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", null)
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
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
  public void testFieldValueReplacerReplaceNulls() throws StageException {

    FieldValueReplacer.FieldValueReplacerConfig nameReplacement = new FieldValueReplacer.FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/name");
    nameReplacement.newValue = "StreamSets";

    FieldValueReplacer.FieldValueReplacerConfig addressReplacement = new FieldValueReplacer.FieldValueReplacerConfig();
    addressReplacement.fields = ImmutableList.of("/streetAddress");
    addressReplacement.newValue = "Sansome Street";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacer.class)
      .addConfiguration("fieldsToNull", null)
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement, addressReplacement))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
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
      Assert.assertEquals(21, result.get("age").getValue());
      Assert.assertTrue(result.containsKey("streetAddress"));
      Assert.assertEquals("Sansome Street", result.get("streetAddress").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testFieldValueReplacerFieldsToNullAndReplaceNulls() throws StageException {

    FieldValueReplacer.FieldValueReplacerConfig nameReplacement = new FieldValueReplacer.FieldValueReplacerConfig();
    nameReplacement.fields = ImmutableList.of("/name");
    nameReplacement.newValue = "StreamSets";

    FieldValueReplacer.FieldValueReplacerConfig addressReplacement = new FieldValueReplacer.FieldValueReplacerConfig();
    addressReplacement.fields = ImmutableList.of("/streetAddress");
    addressReplacement.newValue = "Sansome Street";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldValueReplacer.class)
      .addConfiguration("fieldsToNull", ImmutableList.of("/age"))
      .addConfiguration("fieldsToReplaceIfNull", ImmutableList.of(nameReplacement, addressReplacement))
      .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
      map.put("age", Field.create(21));
      map.put("streetAddress", Field.create(Field.Type.STRING, null));
      Record record = new RecordImpl("s", "s:1", null, null);
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
}
