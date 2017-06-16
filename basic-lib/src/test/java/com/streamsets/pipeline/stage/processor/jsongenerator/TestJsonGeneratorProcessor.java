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

package com.streamsets.pipeline.stage.processor.jsongenerator;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJsonGeneratorProcessor {

  @Test
  public void testMapField() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      Map<String, Field> toSerialize = new HashMap<>();
      toSerialize.put("a", Field.create("value"));
      toSerialize.put("b", Field.create(1L));
      toSerialize.put("c", Field.create(ImmutableList.of(Field.create("x"), Field.create("y"), Field.create("z"))));
      root.put("a_map", Field.create(toSerialize));
      record.set(Field.create(root));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);

      String expected = "{\"a\":\"value\",\"b\":1,\"c\":[\"x\",\"y\",\"z\"]}";
      assertEquals(expected, output.getRecords().get("out").get(0).get("/json").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testListField() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      Field toSerialize = Field.create(ImmutableList.of(Field.create("x"), Field.create("y"), Field.create("z")));
      root.put("a_map", toSerialize);
      record.set(Field.create(root));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);

      String expected = "[\"x\",\"y\",\"z\"]";
      assertEquals(expected, output.getRecords().get("out").get(0).get("/json").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testListMapField() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      Map<String, Field> toSerialize = new LinkedHashMap<>();
      toSerialize.put("a", Field.create("value"));
      toSerialize.put("b", Field.create(1L));
      toSerialize.put("c", Field.create(ImmutableList.of(Field.create("x"), Field.create("y"), Field.create("z"))));
      root.put("a_map", Field.create(Field.Type.LIST_MAP, toSerialize));
      record.set(Field.create(root));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);

      String expected = "{\"a\":\"value\",\"b\":1,\"c\":[\"x\",\"y\",\"z\"]}";
      assertEquals(expected, output.getRecords().get("out").get(0).get("/json").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMissingRequestedFieldPath() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      record.set(Field.create(new HashMap<>()));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().get("out").isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNullFieldPathValue() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      root.put("a_map", Field.create(Field.Type.MAP, null));
      record.set(Field.create(root));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().get("out").isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testUnsupportedFieldType() throws Exception {
    ProcessorRunner runner = getProcessorRunner();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> root = new HashMap<>();
      root.put("a_map", Field.create(Field.Type.STRING, "abc"));
      record.set(Field.create(root));
      List<Record> input = ImmutableList.of(record);

      StageRunner.Output output = runner.runProcess(input);
      assertTrue(output.getRecords().get("out").isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  private static ProcessorRunner getProcessorRunner() {
    return getProcessorRunner(Collections.emptyMap());
  }

  private static ProcessorRunner getProcessorRunner(@NotNull Map<String, String> additionalConfiguration) {
    ProcessorRunner.Builder builder = new ProcessorRunner.Builder(JsonGeneratorDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("fieldPathToSerialize", "/a_map")
        .addConfiguration("outputFieldPath", "/json")
        .addOutputLane("out");

    additionalConfiguration.forEach(builder::addConfiguration);
    return builder.build();
  }

}