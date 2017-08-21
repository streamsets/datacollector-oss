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
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.fieldbuilder.MapFieldBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class TestFieldFlattenerProcessorSpecificFields {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Record runPipeline(ProcessorRunner runner, Record inputRecord) throws StageException {
    runner.runInit();
    StageRunner.Output output = runner.runProcess(ImmutableList.of(inputRecord));
    final List<Record> records = output.getRecords().get("a");
    assertThat(records, hasSize(1));
    final Record outputRecord = records.get(0);
    runner.runDestroy();
    return outputRecord;
  }

  @Test
  public void testFlattenSpecificField() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.fields = Arrays.asList("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.add("top-level", "top-level-value")
      .startMap("map")
        .startMap("map1").add("map1-1", "a").add("map1-2", "b").end()
        .startMap("map2").add("map2-1", "c").add("map2-2", "d").end()
        .startList("list1").add("first").add("second").add("third").end()
      .end()
      .startList("list").add("list1").add("list2").end();

    record.set(builder.build());

    final Record output = runPipeline(runner, record);

    Field field = output.get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertThat(result.size(), equalTo(3));
    assertThat(result, hasKey("top-level"));
    assertThat(result, hasKey("map"));
    assertThat(result, hasKey("list"));

    Map<String, Field> flattenedMap = output.get("/map").getValueAsMap();
    assertThat(flattenedMap.size(), equalTo(7));
    assertThat(flattenedMap, hasKey("map1.map1-1"));
    assertThat(flattenedMap, hasKey("map1.map1-2"));
    assertThat(flattenedMap, hasKey("map2.map2-1"));
    assertThat(flattenedMap, hasKey("map2.map2-2"));
    assertThat(flattenedMap, hasKey("list1.0"));
    assertThat(flattenedMap, hasKey("list1.1"));
    assertThat(flattenedMap, hasKey("list1.2"));
  }

  @Test
  public void testFlattenMultipleFields() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.fields = Arrays.asList("/map1", "/lists", "/map2/map2-2");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.startMap("map1")
            .startMap("map1-1").add("map1-1-a", "a").add("map1-1-b", "b").end()
            .startMap("map1-2").add("map1-2-c", "c").add("map1-2-d", "d").end()
            .end()
          .startList("lists")
            .startList("list1").add("list1-1").add("list1-2").add("list1-3").end()
            .startList("list2").add("list2-1").add("list2-2").add("list2-3").end()
          .end()
          .startMap("map2")
            .startMap("map2-1")
              .startMap("map2-1-1").add("map2-1-1-a", "a").add("map2-1-1-b", "b").end()
              .startMap("map2-1-2").add("map2-1-2-c", "c").add("map2-1-2-d", "d").end()
            .end()
            .startMap("map2-2")
              .startMap("map2-2-1").add("map2-2-1-a", "a").add("map2-2-1-b", "b").end()
              .startMap("map2-2-2").add("map2-2-2-c", "c").add("map2-2-2-d", "d").end()
            .end()
          .end()
          .startList("list3").add("list3-1").add("list3-2").add("list3-3").end();

    record.set(builder.build());

    final Record output = runPipeline(runner, record);

    Field field = output.get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertThat(result.size(), equalTo(4));
    assertThat(result, hasKey("map1"));
    assertThat(result, hasKey("lists"));
    assertThat(result, hasKey("map2"));
    assertThat(result, hasKey("list3"));

    Map<String, Field> map1 = output.get("/map1").getValueAsMap();
    assertThat(map1.size(), equalTo(4));
    assertThat(map1, hasKey("map1-1.map1-1-a"));
    assertThat(map1, hasKey("map1-1.map1-1-b"));
    assertThat(map1, hasKey("map1-2.map1-2-c"));
    assertThat(map1, hasKey("map1-2.map1-2-d"));

    Map<String, Field> lists = output.get("/lists").getValueAsMap();
    assertThat(lists.size(), equalTo(6));
    assertThat(lists, hasKey("0.0"));
    assertThat(lists, hasKey("0.1"));
    assertThat(lists, hasKey("0.2"));
    assertThat(lists, hasKey("1.0"));
    assertThat(lists, hasKey("1.1"));
    assertThat(lists, hasKey("1.2"));

    // ensure map2-1 wasn't touched
    Map<String, Field> map2_1 = output.get("/map2/map2-1").getValueAsMap();
    assertThat(map2_1.size(), equalTo(2));
    assertThat(map2_1, hasKey("map2-1-1"));
    assertThat(map2_1, hasKey("map2-1-2"));

    // map2-2 should be flattened
    Map<String, Field> map2_2 = output.get("/map2/map2-2").getValueAsMap();
    assertThat(map2_2.size(), equalTo(4));
    assertThat(map2_2, hasKey("map2-2-1.map2-2-1-a"));
    assertThat(map2_2, hasKey("map2-2-1.map2-2-1-b"));
    assertThat(map2_2, hasKey("map2-2-2.map2-2-2-c"));
    assertThat(map2_2, hasKey("map2-2-2.map2-2-2-d"));
  }

  @Test
  public void testNonExistentField() throws Exception {
    thrown.expect(OnRecordErrorException.class);

    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.fields = Arrays.asList("/something");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder
      .add("top-level", "top-level-value")
      .startMap("map")
        .startMap("map1").add("map1-1", "a").add("map1-2", "b").end()
        .startMap("map2").add("map2-1", "c").add("map2-2", "d").end()
        .startList("list1").add("first").add("second").add("third").end()
      .end()
    .startList("list").add("list1").add("list2").end();

    record.set(builder.build());

    runPipeline(runner, record);

    fail("Should not have been able to flatten non-existent field");
  }

}
