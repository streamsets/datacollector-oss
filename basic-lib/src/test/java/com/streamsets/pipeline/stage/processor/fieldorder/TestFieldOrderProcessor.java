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
package com.streamsets.pipeline.stage.processor.fieldorder;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.fieldorder.config.DataType;
import com.streamsets.pipeline.stage.processor.fieldorder.config.ExtraFieldAction;
import com.streamsets.pipeline.stage.processor.fieldorder.config.MissingFieldAction;
import com.streamsets.pipeline.stage.processor.fieldorder.config.OrderConfigBean;
import com.streamsets.pipeline.stage.processor.fieldorder.config.OutputType;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFieldOrderProcessor {

  @Test
  public void testDuplicatesInFieldsAndDiscardFields() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.discardFields = ImmutableList.of("/a");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    String error = issues.get(0).toString();
    assertTrue(Utils.format("Message: {}", error), error.contains("/a") && error.contains("FIELD_ORDER_003"));
  }

  @Test
  public void testListMap() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.outputType = OutputType.LIST_MAP;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("b"));
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Record r = output.getRecords().get("a").get(0);
    assertEquals(Field.Type.LIST_MAP, r.get().getType());

    LinkedHashMap<String, Field> m = r.get().getValueAsListMap();
    Iterator<Map.Entry<String, Field>> it =  m.entrySet().iterator();

    Map.Entry<String, Field> entry = it.next();
    assertEquals("a", entry.getKey());
    assertEquals("a", entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals("b", entry.getKey());
    assertEquals("b", entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals("c", entry.getKey());
    assertEquals("c", entry.getValue().getValueAsString());

    assertFalse(it.hasNext());

    runner.runDestroy();
  }

  @Test
  public void testListMapWithQuotesAndSpacesInFieldNames() throws Exception {

    OrderConfigBean config = new OrderConfigBean();

    String field1 = "first field";
    String field1Path = String.format("/'%s'", field1);
    String field2 = "'second field'";
    String field2Path = String.format("/'%s'", field2.replaceAll("'", "\\\\\\\\'"));
    String field3 = "third field";
    String field3Path = String.format("/'%s'", field3);

    config.fields = ImmutableList.of(field1Path, field2Path, field3Path);
    config.outputType = OutputType.LIST_MAP;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
        .addOutputLane("a")
        .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put(field2, Field.create(field2));
    map.put(field3, Field.create(field3));
    map.put(field1, Field.create(field1));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Record r = output.getRecords().get("a").get(0);
    assertEquals(Field.Type.LIST_MAP, r.get().getType());

    LinkedHashMap<String, Field> m = r.get().getValueAsListMap();
    Iterator<Map.Entry<String, Field>> it =  m.entrySet().iterator();

    Map.Entry<String, Field> entry = it.next();
    assertEquals(field1, entry.getKey());
    assertEquals(field1, entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals(field2, entry.getKey());
    assertEquals(field2, entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals(field3, entry.getKey());
    assertEquals(field3, entry.getValue().getValueAsString());

    assertFalse(it.hasNext());

    List<Field> list = r.get().getValueAsList();
    assertEquals(3, list.size());
    assertEquals(field1, list.get(0).getValueAsString());
    assertEquals(field2, list.get(1).getValueAsString());
    assertEquals(field3, list.get(2).getValueAsString());

    runner.runDestroy();
  }

  @Test
  public void testList() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.outputType = OutputType.LIST;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("b"));
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Record r = output.getRecords().get("a").get(0);
    assertEquals(Field.Type.LIST, r.get().getType());

    List<Field> list = r.get().getValueAsList();
    assertEquals(3, list.size());
    assertEquals("a", list.get(0).getValueAsString());
    assertEquals("b", list.get(1).getValueAsString());
    assertEquals("c", list.get(2).getValueAsString());

    runner.runDestroy();
  }

  @Test
  public void testDefaultOnMissingField() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.missingFieldAction = MissingFieldAction.USE_DEFAULT;
    config.defaultValue = "missing";
    config.dataType = DataType.STRING;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("b"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Record r = output.getRecords().get("a").get(0);
    assertEquals(Field.Type.LIST_MAP, r.get().getType());

    LinkedHashMap<String, Field> m = r.get().getValueAsListMap();
    Iterator<Map.Entry<String, Field>> it =  m.entrySet().iterator();

    Map.Entry<String, Field> entry = it.next();
    assertEquals("a", entry.getKey());
    assertEquals("missing", entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals("b", entry.getKey());
    assertEquals("b", entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals("c", entry.getKey());
    assertEquals("missing", entry.getValue().getValueAsString());

    assertFalse(it.hasNext());

    runner.runDestroy();
  }

  @Test
  public void testToErrorOnMissingField() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(0, output.getRecords().get("a").size());
    assertEquals(1, runner.getErrorRecords().size());
  }

  @Test
  public void testToErrorOnExtraField() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("c"));
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    map.put("d", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(0, output.getRecords().get("a").size());
    assertEquals(1, runner.getErrorRecords().size());
  }

  @Test
  public void testDiscardOnExtraField() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.extraFieldAction = ExtraFieldAction.DISCARD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("c"));
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    map.put("d", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());
    assertEquals(0, runner.getErrorRecords().size());
  }

  @Test
  public void testIgnoreExtraFields() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/a", "/b", "/c");
    config.discardFields = ImmutableList.of("/d");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("b", Field.create("c"));
    map.put("c", Field.create("c"));
    map.put("a", Field.create("a"));
    map.put("d", Field.create("a"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());
    assertEquals(0, runner.getErrorRecords().size());
  }

  @Test
  public void testWorkWithNestedTypes() throws Exception {
    OrderConfigBean config = new OrderConfigBean();
    config.fields = ImmutableList.of("/map/a", "/map/b");
    config.discardFields = ImmutableList.of("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldOrderDProcessor.class, new FieldOrderProcessor(config))
      .addOutputLane("a")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    Map<String, Field> innerMap = new LinkedHashMap<>();
    innerMap.put("a", Field.create("a"));
    innerMap.put("b", Field.create("b"));


    Map<String, Field> rootMap = new LinkedHashMap<>();
    rootMap.put("map", Field.create(Field.Type.LIST_MAP, innerMap));

    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, rootMap));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());
    assertEquals(0, runner.getErrorRecords().size());

    Record r = output.getRecords().get("a").get(0);
    assertEquals(Field.Type.LIST_MAP, r.get().getType());

    LinkedHashMap<String, Field> m = r.get().getValueAsListMap();
    Iterator<Map.Entry<String, Field>> it =  m.entrySet().iterator();

    Map.Entry<String, Field> entry = it.next();
    assertEquals("map.a", entry.getKey());
    assertEquals("a", entry.getValue().getValueAsString());

    entry = it.next();
    assertEquals("map.b", entry.getKey());
    assertEquals("b", entry.getValue().getValueAsString());

    assertFalse(it.hasNext());
  }
}
