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
package com.streamsets.pipeline.stage.processor.fieldflattener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFieldFlattenerEntireRecordProcessor {

  @Test
  public void testFlattenEntireRecord() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
      .put("top-level", Field.create(Field.Type.STRING, "top-level"))
      .put("map", Field.create(Field.Type.MAP, ImmutableMap.builder()
        .put("map1", Field.create(Field.Type.STRING, "map1"))
        .put("map2", Field.create(Field.Type.STRING, "map2"))
        .build()))
      .put("list", Field.create(Field.Type.LIST, ImmutableList.of(
        Field.create(Field.Type.STRING, "list1"),
        Field.create(Field.Type.STRING, "list2"))))
      .build()
    ));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertEquals(5, result.size());
    assertTrue(result.containsKey("top-level"));
    assertTrue(result.containsKey("map.map1"));
    assertTrue(result.containsKey("map.map2"));
    assertTrue(result.containsKey("list.0"));
    assertTrue(result.containsKey("list.1"));

    runner.runDestroy();
  }

  @Test
  public void testFlattenEntireRecordDifferentSeparator() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = "/";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
      .put("top-level", Field.create(Field.Type.STRING, "top-level"))
      .put("map", Field.create(Field.Type.MAP, ImmutableMap.builder()
        .put("map1", Field.create(Field.Type.STRING, "map1"))
        .put("map2", Field.create(Field.Type.STRING, "map2"))
        .build()))
      .put("list", Field.create(Field.Type.LIST, ImmutableList.of(
        Field.create(Field.Type.STRING, "list1"),
        Field.create(Field.Type.STRING, "list2"))))
      .build()
    ));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertEquals(5, result.size());
    assertTrue(result.containsKey("top-level"));
    assertTrue(result.containsKey("map/map1"));
    assertTrue(result.containsKey("map/map2"));
    assertTrue(result.containsKey("list/0"));
    assertTrue(result.containsKey("list/1"));

    runner.runDestroy();
  }

  @Test
  public void testFlattenEntireRecordMapInLists() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.builder()
      .put("root-list", Field.create(Field.Type.LIST, ImmutableList.of(
        Field.create(Field.Type.MAP, ImmutableMap.builder()
          .put("a", Field.create(Field.Type.STRING, "a"))
          .put("b", Field.create(Field.Type.STRING, "b"))
          .build()),
        Field.create(Field.Type.MAP, ImmutableMap.builder()
          .put("c", Field.create(Field.Type.STRING, "c"))
          .put("d", Field.create(Field.Type.STRING, "d"))
          .build()))))
      .build()
    ));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertEquals(4, result.size());
    assertTrue(result.containsKey("root-list.0.a"));
    assertTrue(result.containsKey("root-list.0.b"));
    assertTrue(result.containsKey("root-list.1.c"));
    assertTrue(result.containsKey("root-list.1.d"));

    runner.runDestroy();
  }

  @Test
  public void testRootFieldIsPrimitive() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.STRING, "key"));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.STRING, field.getType());

    runner.runDestroy();
  }

  @Test
  public void testRootFieldIsNull() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.STRING, null));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.STRING, field.getType());

    runner.runDestroy();
  }

  // SDC-5766
  @Test
  public void tesFlattenNullMap() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.ENTIRE_RECORD;

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
      .addOutputLane("a").build();
    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.MAP, ImmutableMap.of(
      "a", Field.create(Field.Type.MAP, null),
      "b", Field.create(Field.Type.MAP, null)
    )));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    assertEquals(1, output.getRecords().get("a").size());

    Field field = output.getRecords().get("a").get(0).get();
    assertEquals(Field.Type.MAP, field.getType());

    runner.runDestroy();
  }

}
