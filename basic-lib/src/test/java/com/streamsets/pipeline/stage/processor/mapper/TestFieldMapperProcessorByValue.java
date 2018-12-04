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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestFieldMapperProcessorByValue {

  @Test
  public void negate() throws StageException {
    FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_VALUES;
    config.mappingExpression = "${-1 * f:value()}";

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder().listMap(true);
      builder.startMap("values").listMap(true)
          .add("first", 1)
          .add("second", 2)
          .add("third", 3)
          .end();

      Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertAndReturnValues(outputRecord);
      assertThat(outputs, Matchers.mapFieldWithEntry("first", -1));
      assertThat(outputs, Matchers.mapFieldWithEntry("second", -2));
      assertThat(outputs, Matchers.mapFieldWithEntry("third", -3));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void add1ToListItems() throws StageException {
    FieldMapperProcessorConfig config = new FieldMapperProcessorConfig();
    config.operateOn = OperateOn.FIELD_VALUES;
    config.mappingExpression = "${f:value() + 1}";

    final ProcessorRunner runner = new ProcessorRunner.Builder(
        FieldMapperDProcessor.class,
        new FieldMapperProcessor(config)
    ).addOutputLane("a").build();
    runner.runInit();

    try {
      final MapFieldBuilder builder = MapFieldBuilder.builder().listMap(true);
      builder.startList("values")
          .add(10l)
          .add(21l)
          .add(32l)
          .end();

      Record record = RecordCreator.create("s", "s:1");
      record.set(builder.build());

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      final Record outputRecord = output.getRecords().get("a").get(0);
      final Field outputs = assertAndReturnValues(outputRecord, anyOf(equalTo(Field.Type.LIST)));
      assertThat(outputs, Matchers.listFieldWithValues(11l, 22l, 33l));
    } finally {
      runner.runDestroy();
    }
  }

  private static Field assertAndReturnValues(Record record) {
    return assertAndReturnValues(record, TestFieldMapperProcessorByName.IS_A_MAP);
  }

  private static Field assertAndReturnValues(Record record, AnyOf<Field.Type> valuesTypeMatcher) {
    assertThat(record, notNullValue());
    final Field rootField = record.get();
    assertThat(rootField, notNullValue());
    assertThat(rootField.getType(), TestFieldMapperProcessorByName.IS_A_MAP);
    final Field inputs = rootField.getValueAsMap().get("values");
    assertThat(inputs, notNullValue());
    assertThat(inputs.getType(), valuesTypeMatcher);
    return inputs;
  }
}
