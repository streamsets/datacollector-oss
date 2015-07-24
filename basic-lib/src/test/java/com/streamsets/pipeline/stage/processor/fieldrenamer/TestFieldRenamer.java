/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.fieldrenamer;

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
import com.streamsets.pipeline.stage.processor.fieldvaluereplacer.FieldValueReplacerConfig;
import com.streamsets.pipeline.stage.processor.fieldvaluereplacer.FieldValueReplacerDProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldRenamer {

  @Test
  public void testNonExistingSourceAndTargetFields() throws StageException {
    // If neither the source or target fields exist, then field renaming is a noop, and should succeed
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromField = "/nonExisting";
    renameConfig.toField = "/alsoNonExisting";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("name", Field.create(Field.Type.STRING, null));
            Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertTrue(result.containsKey("name"));
      Assert.assertEquals(null, result.get("name").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingSourceFieldSuccess() throws StageException {
    // If source field does not exist, and precondition is set to continue, this should succeed
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromField = "/nonExisting";
    renameConfig.toField = "/existing";

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, null));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertTrue(result.containsKey("existing"));
      Assert.assertEquals(null, result.get("existing").getValue());
    } finally {
      runner.runDestroy();
    }

    runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", true)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, null));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertTrue(result.size() == 1);
      Assert.assertTrue(result.containsKey("existing"));
      Assert.assertEquals(null, result.get("existing").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonExistingSourceFieldError() throws StageException {
    // If precondition failure is set to error, a missing source field should cause an error
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromField = "/nonExisting";
    renameConfig.toField = "/existing";

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("other", Field.create(Field.Type.STRING, null));
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
  public void testOverwriteSuccess() throws StageException {
    // Standard overwrite condition. Source and target fields exist
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromField = "/existing";
    renameConfig.toField = "/overwrite";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", true)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, "foo"));
      map.put("overwrite", Field.create(Field.Type.STRING, "bar"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertTrue(result.containsKey("overwrite"));
      Assert.assertTrue(!result.containsKey("existing"));
      Assert.assertEquals("foo", result.get("overwrite").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOverwriteError() throws StageException {
    // If overwrite is set to false, overwriting should result in an error
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromField = "/existing";
    renameConfig.toField = "/overwrite";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.CONTINUE)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, "foo"));
      map.put("overwrite", Field.create(Field.Type.STRING, "bar"));
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
  public void testRename() throws StageException {
    // Straightforward rename -- existing source, non-existing target
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromField = "/existing";
    renameConfig1.toField = "/nonExisting";
    FieldRenamerConfig renameConfig2 = new FieldRenamerConfig();
    renameConfig2.fromField = "/listOfMaps[0]/existing";
    renameConfig2.toField = "/listOfMaps[0]/nonExisting";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class)
        .addConfiguration("renameMapping", ImmutableList.of(renameConfig1, renameConfig2))
        .addConfiguration("onStagePreConditionFailure", OnStagePreConditionFailure.TO_ERROR)
        .addConfiguration("overwriteExisting", false)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, "foo"));
      map.put("listOfMaps", Field.create(ImmutableList.of(Field.create(ImmutableMap.of("existing",
        Field.create(Field.Type.STRING, "foo"))))));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 2, result.size());
      Assert.assertTrue(result.containsKey("nonExisting"));
      Assert.assertFalse(result.containsKey("existing"));
      Assert.assertTrue(result.containsKey("listOfMaps"));
      Assert.assertEquals("foo", result.get("nonExisting").getValue());
      result = result.get("listOfMaps").getValueAsList().get(0).getValueAsMap();
      Assert.assertTrue(result.containsKey("nonExisting"));
      Assert.assertFalse(result.containsKey("existing"));
      Assert.assertEquals("foo", result.get("nonExisting").getValue());
    } finally {
      runner.runDestroy();
    }
  }
}
