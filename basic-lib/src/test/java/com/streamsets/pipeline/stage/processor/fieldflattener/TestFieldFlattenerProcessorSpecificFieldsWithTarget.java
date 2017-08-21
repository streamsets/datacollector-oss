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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertEquals;

public class TestFieldFlattenerProcessorSpecificFieldsWithTarget {

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
  public void testSimpleFlattenToRoot() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.flattenInPlace = false;
    config.flattenTargetField = "/";
    config.fields = Arrays.asList("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.add("top-level", "top-level-value")
      .startMap("map")
        .startMap("map1").add("map1-1", "a").add("map1-2", "b").end()
      .end();

    record.set(builder.build());

    final Record output = runPipeline(runner, record);

    Field field = output.get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertThat(result.size(), equalTo(3));
    assertThat(result, hasKey("top-level"));
    assertThat(result, hasKey("map1.map1-1"));
    assertThat(result, hasKey("map1.map1-2"));
  }

  @Test
  public void testCollisionDiscard() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.flattenInPlace = false;
    config.flattenTargetField = "/";
    config.collisionFieldAction = CollisionFieldAction.DISCARD;
    config.fields = Arrays.asList("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.add("field", "ROOT")
      .startMap("map")
        .add("field", "MAP")
      .end();

    record.set(builder.build());

    final Record output = runPipeline(runner, record);

    Field field = output.get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertThat(result.size(), equalTo(1));
    assertThat(result, hasKey("field"));
    assertThat(result.get("field").getValueAsString(), equalTo("ROOT"));
  }

  @Test
  public void testCollisionOverride() throws Exception {
    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.flattenInPlace = false;
    config.flattenTargetField = "/";
    config.collisionFieldAction = CollisionFieldAction.OVERRIDE;
    config.fields = Arrays.asList("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.add("field", "ROOT")
      .startMap("map")
        .add("field", "MAP")
      .end();

    record.set(builder.build());

    final Record output = runPipeline(runner, record);

    Field field = output.get();
    assertEquals(Field.Type.MAP, field.getType());

    Map<String, Field> result = field.getValueAsMap();
    assertThat(result.size(), equalTo(1));
    assertThat(result, hasKey("field"));
    assertThat(result.get("field").getValueAsString(), equalTo("MAP"));
  }

  @Test
  public void testCollisionToError() throws Exception {
    thrown.expect(OnRecordErrorException.class);

    FieldFlattenerConfig config = new FieldFlattenerConfig();
    config.nameSeparator = ".";
    config.flattenType = FlattenType.SPECIFIC_FIELDS;
    config.flattenInPlace = false;
    config.flattenTargetField = "/";
    config.collisionFieldAction = CollisionFieldAction.TO_ERROR;
    config.fields = Arrays.asList("/map");

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldFlattenerDProcessor.class, new FieldFlattenerProcessor(config))
            .addOutputLane("a").build();

    Record record = RecordCreator.create();
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.add("field", "ROOT")
      .startMap("map")
        .add("field", "MAP")
      .end();

    record.set(builder.build());

    runPipeline(runner, record);
  }

}
