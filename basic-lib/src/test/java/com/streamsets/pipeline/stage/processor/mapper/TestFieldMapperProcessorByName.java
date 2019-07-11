/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.processor.mapper;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.Matchers;
import com.streamsets.testing.fieldbuilder.MapFieldBuilder;
import org.hamcrest.core.AnyOf;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestFieldMapperProcessorByName {

  public static final AnyOf<Field.Type> IS_A_MAP = anyOf(equalTo(Field.Type.MAP), equalTo(Field.Type.LIST_MAP));

  @Test
  public void toList() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/values";
//    config.conditionalExpression = "${str:startsWith(f:path(), '/inputs')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("first", 1)
          .add("second", 2)
          .add("third", 3)
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsEmptyAndReturnOutputs(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("values", 1, 2, 3));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void move() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/${f:name()}";
    config.conditionalExpression = "${str:startsWith(f:path(), '/inputs')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("first", 1)
          .add("second", 2)
          .add("third", 3)
          .end().startListMap("leaveMeAlone")
          .add("important1", "foo")
          .add("important2", "bar")
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsEmptyAndReturnOutputs(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("first", 1));
      assertThat(outputs, Matchers.mapFieldWithEntry("second", 2));
      assertThat(outputs, Matchers.mapFieldWithEntry("third", 3));

      // make sure the "leaveMeAlone" map was left alone
      final Field leaveMeAloneMap = outputRecord.get("/leaveMeAlone");
      assertThat(leaveMeAloneMap, notNullValue());
      assertThat(leaveMeAloneMap, Matchers.mapFieldWithEntry("important1", "foo"));
      assertThat(leaveMeAloneMap, Matchers.mapFieldWithEntry("important2", "bar"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void groupByType() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/${f:type()}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("a", 1)
          .add("b", "bee")
          .add("c", 3.0)
          .add("d", 4)
          .add("e", "eee")
          .add("f", 6.0)
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsEmptyAndReturnOutputs(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("STRING", "bee", "eee"));
      assertThat(outputs, Matchers.mapFieldWithEntry("INTEGER", 1, 4));
      assertThat(outputs, Matchers.mapFieldWithEntry("DOUBLE", 3.0, 6.0));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void groupByTypeAndName() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/${f:type()}/${f:name()}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    final Integer[] integerValues = new Integer[] { 17, 18, 19, 20 };
    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs")
          .startListMap(Field.Type.INTEGER.name()).end()
          .startListMap(Field.Type.DOUBLE.name()).end()
          .startListMap(Field.Type.STRING.name()).end()
          .end().startListMap("inputs")
          .add("a", 1)
          .add("b", "bee")
          .add("c", 3.0d)
          .add("d", 4)
          .add("e", "eee")
          .add("f", 6.0d)
          .startList("listOfValues").add(integerValues).end()
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertThat(outputs, notNullValue());
      assertThat(outputs.getType(), IS_A_MAP);
      final Field strings = outputs.getValueAsMap().get(Field.Type.STRING.name());
      assertThat(strings, notNullValue());
      assertThat(strings,  Matchers.mapFieldWithEntry("b", "bee"));
      assertThat(strings,  Matchers.mapFieldWithEntry("e", "eee"));

      final Field integers = outputs.getValueAsMap().get(Field.Type.INTEGER.name());
      assertThat(integers, notNullValue());
      assertThat(integers,  Matchers.mapFieldWithEntry("a", 1));
      assertThat(integers,  Matchers.mapFieldWithEntry("d", 4));
      assertThat(integers,  Matchers.mapFieldWithEntry("listOfValues", Arrays.asList(integerValues)));

      final Field doubles = outputs.getValueAsMap().get(Field.Type.DOUBLE.name());
      assertThat(doubles, notNullValue());
      assertThat(doubles,  Matchers.mapFieldWithEntry("c", 3.0d));
      assertThat(doubles,  Matchers.mapFieldWithEntry("f", 6.0d));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void sumValues() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/total";
    config.aggregationExpression = "${sum(fields)}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("value1", 1)
          .add("value2", 1)
          .add("value3", 2)
          .add("value4", 3)
          .add("value5", 5)
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsEmptyAndReturnOutputs(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("total", 12));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void gatherStringsAsList() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/values";
    config.aggregationExpression = "${fields}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("one", "foo")
          .add("two", "bar")
          .add("three", "baz")
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsEmptyAndReturnOutputs(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("values", "foo", "bar", "baz"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void joinStrings() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/joined";
    config.aggregationExpression = "${list:join(fields, ',')}";
    config.structureChangeAllowed = true;
    config.maintainOriginalPaths = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("one", "foo")
          .add("two", "bar")
          .add("three", "baz")
          .end();

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertThat(outputs, Matchers.mapFieldWithEntry("joined", "foo,bar,baz"));

      final Field inputs = outputRecord.get("/inputs");
      assertThat(inputs, notNullValue());
      assertThat(inputs, Matchers.mapFieldWithEntry("one", "foo"));
      assertThat(inputs, Matchers.mapFieldWithEntry("two", "bar"));
      assertThat(inputs, Matchers.mapFieldWithEntry("three", "baz"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void findBars() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/bars";
    config.conditionalExpression = "${f:name() == 'bar'}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("foo", 1)
          .add("bar", 2)
          .add("baz", 3)
          .startListMap("submap1")
          .add("s1", 4)
          .add("s2", 5)
          .add("bar", 6)
          .startListMap("submap1_1")
          .add("s1_1", 7)
          .add("s1_2", 8)
          .add("s1_3", 9)
          .add("bar", 10)
          .end() // end submap1_1
          .end() // end submap1
          .end(); // end inputs

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertThat(outputs, Matchers.mapFieldWithEntry("bars", 2, 6, 10));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void replaceSpecialChars() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_NAMES;
    // replace dots, carets, and slashes in field names with underscore
    config.mappingExpression = "${str:replaceAll(f:name(), '[\\\\/.\\\\^]', '_')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("foo", 1)
          .add("bar", 2)
          .add("b^z", 3)
          .startListMap("submap1")
          .add("s^1", 4)
          .add("s2", 5)
          .startListMap("submap1/1")
          .add("s1^1", 7)
          .add("s12", 8)
          .add("s1/3", 9)
          .end() // end submap1_1
          .end() // end submap1
          .end(); // end inputs

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertTrue(outputs.getValueAsMap().isEmpty());
      final Field inputs = outputRecord.get("/inputs");
      assertThat(inputs, notNullValue());
      assertThat(inputs.getType(), IS_A_MAP);
      assertThat(inputs, Matchers.mapFieldWithEntry("foo", 1));
      assertThat(inputs, Matchers.mapFieldWithEntry("bar", 2));
      assertThat(inputs, Matchers.mapFieldWithEntry("b_z", 3));
      final Field submap1 = inputs.getValueAsMap().get("submap1");
      assertThat(submap1, notNullValue());
      assertThat(submap1.getType(), IS_A_MAP);
      assertThat(submap1, Matchers.mapFieldWithEntry("s_1", 4));
      assertThat(submap1, Matchers.mapFieldWithEntry("s2", 5));
      final Field submap1_1 = submap1.getValueAsMap().get("submap1_1");
      assertThat(submap1_1, notNullValue());
      assertThat(submap1_1.getType(), IS_A_MAP);
      assertThat(submap1_1, Matchers.mapFieldWithEntry("s1_1", 7));
      assertThat(submap1_1, Matchers.mapFieldWithEntry("s12", 8));
      assertThat(submap1_1, Matchers.mapFieldWithEntry("s1_3", 9));

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void changeNamesWithLists() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_NAMES;
    // replace dots, carets, and slashes in field names with underscore
    config.mappingExpression = "${str:replaceAll(f:name(), '[A-Z]', '_')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs")
          .add("Foo", 1)
          .add("Bar", 2)
          .startList("BazList")
          .add("listItem1")
          .add("listItem2")
          .add("listItem3")
          .end() // end BazList
          .end(); // end inputs

      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertTrue(outputs.getValueAsMap().isEmpty());
      final Field inputs = outputRecord.get("/inputs");
      assertThat(inputs, notNullValue());
      assertThat(inputs.getType(), IS_A_MAP);
      assertThat(inputs, Matchers.mapFieldWithEntry("_oo", 1));
      assertThat(inputs, Matchers.mapFieldWithEntry("_ar", 2));
      final Field bazList = inputs.getValueAsMap().get("_az_ist");
      assertThat(bazList, notNullValue());
      assertThat(bazList.getType(), equalTo(Field.Type.LIST));
      assertThat(bazList, Matchers.listFieldWithValues("listItem1", "listItem2", "listItem3"));
    } finally {
      runner.runDestroy();
    }
  }

  private final String firstSuspiciousName = "^LULZ.";
  private final String secondSuspiciousName = "; DROP TABLE";

  private final String firstSuspiciousNamePath = "/inputs/'third name'";
  private final String secondSuspiciousNamePath = "/inputs/nested/'definitely not a name'";

  private Record createSuspiciousNameRecord() {
    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.startListMap("outputs").startList("suspiciousNames").end().end().startListMap("inputs")
        .add("first name", "Joe")
        .add("second name", "Bob")
        .add("not a name", 14)
        .add("third name", firstSuspiciousName)
        .startListMap("nested")
        .add("innocuous name", "Lester")
        .add("definitely not a name", secondSuspiciousName)
        .end()
        .end();

    final Record record = RecordCreator.create("s", "s:1");
    record.set(builder.build());
    return record;
  }

  @Test
  public void gatherSuspiciousNames() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/suspiciousNames";
    config.aggregationExpression = "${asFields(map(fields, previousPath()))}";
    config.conditionalExpression = "${f:type() == 'STRING' and str:contains(f:name(), 'name') and " +
        "str:matches(f:value(), '^[;\\\\-*\".&*\\\\^].*')}";
    config.structureChangeAllowed = true;
    config.maintainOriginalPaths = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final Record record = createSuspiciousNameRecord();

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertThat(outputs, Matchers.mapFieldWithEntry(
          "suspiciousNames",
          firstSuspiciousNamePath,
          secondSuspiciousNamePath
      ));
      // since maintainOriginalPaths was set, the original field paths should still be in the output record
      assertThat(outputRecord.get(firstSuspiciousNamePath), Matchers.fieldWithValue(firstSuspiciousName));
      assertThat(outputRecord.get(secondSuspiciousNamePath), Matchers.fieldWithValue(secondSuspiciousName));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void gatherSuspiciousNameFieldByOriginalPath() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/suspiciousNames";
    config.aggregationExpression = "${map(fields, fieldByPreviousPath())}";
    config.conditionalExpression = "${f:type() == 'STRING' and str:contains(f:name(), 'name') and " +
        "str:matches(f:value(), '^[;\\\\-*\".&*\\\\^].*')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final Record record = createSuspiciousNameRecord();

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      final Field suspiciousNamesField = outputs.getValueAsMap().get("suspiciousNames");
      assertThat(suspiciousNamesField, Matchers.listFieldWithSize(2));

      final Field firstSuspiciousNameField = suspiciousNamesField.getValueAsList().get(0);
      assertThat(firstSuspiciousNameField, Matchers.mapFieldWithEntry(
          firstSuspiciousNamePath,
          firstSuspiciousName
      ));

      final Field secondSuspiciousNameField = suspiciousNamesField.getValueAsList().get(1);
      assertThat(secondSuspiciousNameField, Matchers.mapFieldWithEntry(
          secondSuspiciousNamePath,
          secondSuspiciousName
      ));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void createFieldsBySiblings() throws StageException {
    final FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_PATHS;
    config.mappingExpression = "/outputs/foo_value";
    config.conditionalExpression = "${str:startsWith(f:path(), '/inputs') and f:name() == 'AttributeValue' and f:hasSiblingWithValue('AttributeName', 'foo')}";
    config.structureChangeAllowed = true;

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder();
      builder.startListMap("outputs").end().startListMap("inputs").startList("dataItems")
          .startListMap("data")
          .add("AttributeName", "attr1")
          .add("AttributeValue", "val1")
          .end()
          .startListMap("data")
          .add("AttributeName", "attr2")
          .add("AttributeValue", "val2")
          .end()
          .startListMap("data")
          .add("AttributeName", "foo")
          .add("AttributeValue", "bar")
          .end()
          .end() // end dataItems list
          .end(); // end inputs map


      final Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      final StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertInputsAndReturnOutputs(outputRecord, false);
      assertThat(outputs, Matchers.mapFieldWithEntry("foo_value", "bar"));
    } finally {
      runner.runDestroy();
    }
  }

  private static Field assertInputsEmptyAndReturnOutputs(Record record) {
    return assertInputsAndReturnOutputs(record, true);
  }

  private static Field assertInputsAndReturnOutputs(Record record, boolean inputsEmpty) {
    assertThat(record, notNullValue());
    final Field rootField = record.get();
    assertThat(rootField, notNullValue());
    assertThat(rootField.getType(), IS_A_MAP);
    final Field inputs = rootField.getValueAsMap().get("inputs");
    assertThat(inputs, notNullValue());
    assertThat(inputs.getType(), IS_A_MAP);
    if (inputsEmpty) {
      assertTrue(inputs.getValueAsMap().isEmpty());
    } else {
      assertFalse(inputs.getValueAsMap().isEmpty());
    }
    final Field outputs = rootField.getValueAsMap().get("outputs");
    assertThat(outputs, notNullValue());
    return outputs;
  }
}
