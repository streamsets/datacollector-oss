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
package com.streamsets.pipeline.stage.processor.fieldmerger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestFieldMerger {

  @Test
  public void testNonExistingSourceField() throws StageException {
    // If the source field does not exist, precondition failure config will determine success
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/nonExisting";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("target", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMapMerge() throws StageException {
    // For merging maps, source field should be removed, and contained fields will be merged into
    // the target map
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          "b", Field.create("bval"));
      Map<String, Field> targetMap = ImmutableMap.of(
          "c", Field.create("cval"),
          "d", Field.create("dval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      map.put("target", Field.create(Field.Type.MAP, targetMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("target"));
      Assert.assertTrue(!result.containsKey("source"));
      Map<String, Field> targetVal = result.get("target").getValueAsMap();
      Assert.assertEquals(4, targetVal.keySet().size());
      Assert.assertTrue(targetVal.containsKey("a"));
      Assert.assertTrue(targetVal.containsKey("b"));
      Assert.assertTrue(targetVal.containsKey("c"));
      Assert.assertTrue(targetVal.containsKey("d"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
     public void testMapMergeToRoot() throws StageException {
    // For merging maps, source field should be removed, and contained fields will be merged into
    // the target map
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          "b", Field.create("bval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 2);
      Assert.assertTrue(!result.containsKey(mergeConfig.fromField));
      Assert.assertTrue(result.containsKey("a"));
      Assert.assertTrue(result.containsKey("b"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMissingTargetParent() throws StageException {
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/parent/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          "b", Field.create("bval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMapMergeOverwriteSuccess() throws StageException {
    // If source and target contain fields with the same name, overwrite determines success
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", true)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      String overwrittenKey = "c";
      String overwrittenValue = "cval_overwritten";
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          overwrittenKey, Field.create(overwrittenValue));
      Map<String, Field> targetMap = ImmutableMap.of(
          overwrittenKey, Field.create("cval"),
          "d", Field.create("dval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      map.put("target", Field.create(Field.Type.MAP, targetMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("target"));
      Assert.assertTrue(!result.containsKey("source"));
      Map<String, Field> targetVal = result.get("target").getValueAsMap();
      Assert.assertEquals(3, targetVal.keySet().size());
      Assert.assertEquals(overwrittenValue, targetVal.get(overwrittenKey).getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMapMergeOverwriteError() throws StageException {
    // Case where overwrite is required, but config is set to disallow overwriting
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      String overwrittenKey = "c";
      String overwrittenValue = "cval_overwritten";
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          overwrittenKey, Field.create(overwrittenValue));
      Map<String, Field> targetMap = ImmutableMap.of(
          overwrittenKey, Field.create("cval"),
          "d", Field.create("dval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      map.put("target", Field.create(Field.Type.MAP, targetMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testIncompatibleTypes() throws StageException {
    // Maps can merge into maps, and lists into lists
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> sourceMap = ImmutableMap.of(
          "a", Field.create("aval"),
          "b", Field.create("bval"));
      List<Field> targetMap = ImmutableList.of(
          Field.create("cval"),
          Field.create("dval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.MAP, sourceMap));
      map.put("target", Field.create(Field.Type.LIST, targetMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testListMerge() throws StageException {
    // Merging lists will concatenate all source elements at end of target list
    FieldMergerConfig mergeConfig = new FieldMergerConfig();
    mergeConfig.fromField = "/source";
    mergeConfig.toField = "/target";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldMergerDProcessor.class)
        .addConfiguration("mergeMapping", ImmutableList.of(mergeConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      List<Field> sourceMap = ImmutableList.of(
          Field.create("aval"),
          Field.create("bval"));
      List<Field> targetMap = ImmutableList.of(
          Field.create("cval"),
          Field.create("dval"));

      Map<String, Field> map = new LinkedHashMap<>();
      map.put("source", Field.create(Field.Type.LIST, sourceMap));
      map.put("target", Field.create(Field.Type.LIST, targetMap));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("target"));
      Assert.assertTrue(!result.containsKey("source"));
      List<Field> targetVal = result.get("target").getValueAsList();
      Assert.assertEquals(4, targetVal.size());
    } finally {
      runner.runDestroy();
    }
  }
}
