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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestFieldRenamer {

  @Test
  public void testNonExistingSourceAndTargetFields() throws StageException {
    // If neither the source or target fields exist, then field renaming is a noop, and should succeed
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromFieldExpression = "/nonExisting";
    renameConfig.toFieldExpression = "/alsoNonExisting";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;
    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
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
    renameConfig.fromFieldExpression = "/nonExisting";
    renameConfig.toFieldExpression = "/existing";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
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

    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
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
    renameConfig.fromFieldExpression = "/nonExisting";
    renameConfig.toFieldExpression = "/existing";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
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
  public void testTargetFieldExistsReplace() throws StageException {
    // Standard overwrite condition. Source and target fields exist
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromFieldExpression = "/existing";
    renameConfig.toFieldExpression = "/overwrite";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
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
  public void testTargetFieldExistsReplaceItself() throws StageException {
    // Standard overwrite condition. Source and target fields are the same
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromFieldExpression = "/existing";
    renameConfig.toFieldExpression = "/existing";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, "foo"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertTrue(result.containsKey("existing"));
      Assert.assertEquals("foo", result.get("existing").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTargetFieldExistsError() throws StageException {
    // If overwrite is set to false, overwriting should result in an error
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromFieldExpression = "/existing";
    renameConfig.toFieldExpression = "/overwrite";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
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
  public void testTargetFieldExistsAppendNumbers() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/field";
    renameConfig.toFieldExpression = "/col";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.APPEND_NUMBERS;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("field", Field.create(Field.Type.STRING, "field"));
      map.put("col", Field.create(Field.Type.STRING, "col"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(Field.Type.MAP, map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertFalse(r.has("/field"));
      Assert.assertTrue(r.has("/col"));
      Assert.assertTrue(r.has("/col1"));
      Assert.assertEquals("col", r.get("/col").getValueAsString());
      Assert.assertEquals("field", r.get("/col1").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testExpressions() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/(.*)";
    renameConfig.toFieldExpression = "/${str:toUpper(\"$1\")}";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.APPEND_NUMBERS;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("field", Field.create(Field.Type.STRING, "field"));
      map.put("col", Field.create(Field.Type.STRING, "col"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(Field.Type.MAP, map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertTrue(r.has("/FIELD"));
      Assert.assertTrue(r.has("/COL"));
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testMultipleRegexMatchingSameField() throws StageException {
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/sql(.*)";
    renameConfig1.toFieldExpression = "/sqlRename$1";

    FieldRenamerConfig renamerConfig2 = new FieldRenamerConfig();
    renamerConfig2.fromFieldExpression = "/s(.*)";
    renamerConfig2.toFieldExpression = "/sRename$1";

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("sqlField", Field.create(Field.Type.STRING, "foo"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    try {
      FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
      errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
      errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
      errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

      FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig1, renamerConfig2),  errorHandler);

      // Test non-existent source with existing target field
      ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
          .setOnRecordError(OnRecordError.STOP_PIPELINE)
          .addOutputLane("a").build();
      runner.runInit();

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.fail("Should throw error if multiple regex match the same field");
    } catch (OnRecordErrorException e) {
      Assert.assertEquals(Errors.FIELD_RENAMER_03, e.getErrorCode());
    }

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.CONTINUE;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig1, renamerConfig2),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
    Assert.assertEquals(1, output.getRecords().get("a").size());
    Record r = output.getRecords().get("a").get(0);
    Assert.assertTrue(r.has("/sqlField"));
  }

  @Test
  public void testRenameMapField() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/first";
    renameConfig.toFieldExpression = "/second";


    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> renameableInnerMap = new HashMap<>();
    renameableInnerMap.put("value", Field.create(Field.Type.STRING, "value"));

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("first", Field.create(renameableInnerMap));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(Field.Type.MAP, map));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertFalse(r.has("/first"));
      Assert.assertFalse(r.has("/first/value"));
      Assert.assertTrue(r.has("/second"));
      Assert.assertTrue(r.has("/second/value"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testDifferentMatchesRegex() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/'(.*)(#)(.*)'";
    renameConfig.toFieldExpression = "/$1hash$3";


    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.STOP_PIPELINE)
        .addOutputLane("a").build();
    runner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("#abcd", Field.create("hashabcd"));

    Record record1 = RecordCreator.create("s", "s:1");
    record1.set(Field.create(Field.Type.MAP, map));

    map = new LinkedHashMap<>();
    map.put("ab#cd", Field.create("abhashcd"));
    Record record2 = RecordCreator.create("s", "s:2");
    record2.set(Field.create(Field.Type.MAP, map));

    map = new LinkedHashMap<>();
    map.put("abcd#", Field.create("abcdhash"));
    Record record3 = RecordCreator.create("s", "s:3");
    record3.set(Field.create(Field.Type.MAP, map));

    try {
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record1, record2, record3));
      Assert.assertEquals(3, output.getRecords().get("a").size());
      for (Record record : output.getRecords().get("a")) {
        Map<String, Field> fieldMap = record.get().getValueAsMap();
        for (Map.Entry<String, Field> fieldEntry : fieldMap.entrySet()) {
          Assert.assertEquals(fieldEntry.getKey(), fieldEntry.getValue().getValueAsString());
        }
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRegexInNonComplexType() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/'sql(#)(.*)'";
    renameConfig.toFieldExpression = "/sql$2";


    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("sql#1", Field.create(Field.Type.STRING, "foo"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(Field.Type.MAP, map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertFalse(r.has("/'sql#1'"));
      Assert.assertTrue(r.has("/sql1"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRegexInComplexMapType() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes]
    renameConfig.fromFieldExpression = "/(*)/'SQL(#)(.*)'";
    renameConfig.toFieldExpression = "/$1/SQL$3";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> innerMap1 = new LinkedHashMap<>();
      innerMap1.put("SQL#1", Field.create(Field.Type.STRING, "foo1"));

      Map<String, Field> innerMap2 = new LinkedHashMap<>();
      innerMap2.put("SQL#2", Field.create(Field.Type.STRING, "foo2"));

      Map<String, Field> map = new HashMap<>();
      map.put("map1", Field.create(innerMap1));
      map.put("map2", Field.create(innerMap2));


      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);

      Assert.assertFalse(r.getEscapedFieldPaths().contains("/map1/'SQL#1'"));
      Assert.assertFalse(r.getEscapedFieldPaths().contains("/map2/'SQL#2'"));
      Assert.assertTrue(r.getEscapedFieldPaths().contains("/map1/SQL1"));
      Assert.assertTrue(r.getEscapedFieldPaths().contains("/map2/SQL2"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRegexInComplexListType() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/(*)[(*)]/'SQL(#)(.*)'";
    renameConfig.toFieldExpression = "/$1[$2]/SQL$4";


    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.TO_ERROR;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> innerMap1 = new LinkedHashMap<>();
      innerMap1.put("SQL#1", Field.create(Field.Type.STRING, "foo1"));

      Map<String, Field> innerMap2 = new LinkedHashMap<>();
      innerMap2.put("SQL#2", Field.create(Field.Type.STRING, "foo2"));

      List<Field> list = new LinkedList<>();
      list.add(Field.create(innerMap1));
      list.add(Field.create(innerMap2));

      Map<String, Field> map = new HashMap<>();
      map.put("list", Field.create(list));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertFalse(r.getEscapedFieldPaths().contains("/list[0]/'SQL#1'"));
      Assert.assertFalse(r.getEscapedFieldPaths().contains("/list[1]/'SQL#2'"));
      Assert.assertTrue(r.getEscapedFieldPaths().contains("/list[0]/SQL1"));
      Assert.assertTrue(r.getEscapedFieldPaths().contains("/list[1]/SQL2"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCategoryRegexInNonComplexType() throws StageException {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    //Any field containing a non-word character should be in single quotes
    renameConfig.fromFieldExpression = "/'(.*)[#&@|](.*)'";
    renameConfig.toFieldExpression = "/$1_$2";


    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.APPEND_NUMBERS;

    FieldRenamerProcessor processor = new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a#b", Field.create(Field.Type.STRING, "foo1"));
      map.put("a_b", Field.create(Field.Type.STRING, "foo2"));
      map.put("a&b", Field.create(Field.Type.STRING, "foo3"));
      map.put("a|b", Field.create(Field.Type.STRING, "foo4"));
      map.put("a@b", Field.create(Field.Type.STRING, "foo5"));

      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(Field.Type.MAP, map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record r = output.getRecords().get("a").get(0);
      Assert.assertFalse(r.has("/'a#b'"));
      Assert.assertFalse(r.has("/'a&b'"));
      Assert.assertFalse(r.has("/'a|b'"));
      Assert.assertFalse(r.has("/'a&b'"));

      Assert.assertTrue(r.has("/a_b"));
      Assert.assertEquals("foo2" ,r.get("/a_b").getValueAsString());

      Assert.assertTrue(r.has("/a_b1"));
      Assert.assertEquals("foo1" ,r.get("/a_b1").getValueAsString());

      Assert.assertTrue(r.has("/a_b2"));
      Assert.assertEquals("foo3" ,r.get("/a_b2").getValueAsString());

      Assert.assertTrue(r.has("/a_b3"));
      Assert.assertEquals("foo4" ,r.get("/a_b3").getValueAsString());

      Assert.assertTrue(r.has("/a_b4"));
      Assert.assertEquals("foo5" ,r.get("/a_b4").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRenameSimpleAndComplexType() throws StageException {
    // Straightforward rename -- existing source, non-existing target
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/existing";
    renameConfig1.toFieldExpression = "/nonExisting";

    FieldRenamerConfig renameConfig2 = new FieldRenamerConfig();
    renameConfig2.fromFieldExpression = "/listOfMaps[0]/existing";
    renameConfig2.toFieldExpression = "/listOfMaps[0]/nonExisting";

    FieldRenamerConfig renameConfig3 = new FieldRenamerConfig();
    renameConfig3.fromFieldExpression = "/mapOfMaps/(*)/existing";
    renameConfig3.toFieldExpression = "/mapOfMaps/$1/nonExisting";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig1, renameConfig2, renameConfig3),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("existing", Field.create(Field.Type.STRING, "foo"));
      map.put("listOfMaps", Field.create(ImmutableList.of(Field.create(ImmutableMap.of("existing",
          Field.create(Field.Type.STRING, "foo"))))));
      map.put("mapOfMaps",
          Field.create(
              ImmutableMap.of(
                  "innerMap",
                  Field.create(
                      Field.Type.MAP,
                      ImmutableMap.of("existing", Field.create(Field.Type.STRING, "foo")
                      )
                  )
              )
          )
      );
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 3, result.size());
      Assert.assertTrue(result.containsKey("nonExisting"));
      Assert.assertFalse(result.containsKey("existing"));
      Assert.assertTrue(result.containsKey("listOfMaps"));
      Assert.assertTrue(result.containsKey("mapOfMaps"));
      Assert.assertEquals("foo", result.get("nonExisting").getValue());
      Map<String, Field> listResult = result.get("listOfMaps").getValueAsList().get(0).getValueAsMap();
      Assert.assertTrue(listResult.containsKey("nonExisting"));
      Assert.assertFalse(listResult.containsKey("existing"));
      Assert.assertEquals("foo", listResult.get("nonExisting").getValue());
      Assert.assertTrue(result.get("mapOfMaps").getValueAsMap().containsKey("innerMap"));
      Map<String, Field> mapResult= result.get("mapOfMaps").getValueAsMap().get("innerMap").getValueAsMap();
      Assert.assertFalse(mapResult.containsKey("existing"));
      Assert.assertTrue(mapResult.containsKey("nonExisting"));
      Assert.assertEquals("foo", mapResult.get("nonExisting").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRenameMultipleListElementsWithConstIdxExpr() throws StageException {
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/listOfInts[0]";
    renameConfig1.toFieldExpression = "/nonExisting0";

    FieldRenamerConfig renameConfig2 = new FieldRenamerConfig();
    renameConfig2.fromFieldExpression = "/listOfInts[1]";
    renameConfig2.toFieldExpression = "/nonExisting1";

    FieldRenamerConfig renameConfig3 = new FieldRenamerConfig();
    renameConfig3.fromFieldExpression = "/listOfInts[2]";
    renameConfig3.toFieldExpression = "/nonExisting2";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    //Reverse order in configuration so as to preserve array indices
    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig3, renameConfig2, renameConfig1),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("listOfInts",
          Field.create(
              ImmutableList.of(
                  Field.create(Field.Type.INTEGER, 1),
                  Field.create(Field.Type.INTEGER, 2),
                  Field.create(Field.Type.INTEGER, 3)
              )
          )
      );
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Map<String, Field> result = output.getRecords().get("a").get(0).get().getValueAsMap();
      Assert.assertTrue(result.containsKey("listOfInts"));
      Assert.assertTrue(result.get("listOfInts").getValueAsList().isEmpty());
      Assert.assertTrue(result.containsKey("nonExisting0"));
      Assert.assertTrue(result.containsKey("nonExisting1"));
      Assert.assertTrue(result.containsKey("nonExisting2"));
      Assert.assertEquals(1, result.get("nonExisting0").getValueAsInteger());
      Assert.assertEquals(2, result.get("nonExisting1").getValueAsInteger());
      Assert.assertEquals(3, result.get("nonExisting2").getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testRenameMultipleListElementsWithConstIdxExprAndGrp() throws StageException {
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/listOfInts[(0)]";
    renameConfig1.toFieldExpression = "/nonExisting$1";

    FieldRenamerConfig renameConfig2 = new FieldRenamerConfig();
    renameConfig2.fromFieldExpression = "/listOfInts[(1)]";
    renameConfig2.toFieldExpression = "/nonExisting$1";

    FieldRenamerConfig renameConfig3 = new FieldRenamerConfig();
    renameConfig3.fromFieldExpression = "/listOfInts[(2)]";
    renameConfig3.toFieldExpression = "/nonExisting$1";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    //Reverse order in configuration so as to preserve array indices
    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig3, renameConfig2, renameConfig1),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("listOfInts",
          Field.create(
              ImmutableList.of(
                  Field.create(Field.Type.INTEGER, 1),
                  Field.create(Field.Type.INTEGER, 2),
                  Field.create(Field.Type.INTEGER, 3)
              )
          )
      );
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Map<String, Field> result = output.getRecords().get("a").get(0).get().getValueAsMap();
      Assert.assertTrue(result.containsKey("listOfInts"));
      Assert.assertTrue(result.get("listOfInts").getValueAsList().isEmpty());
      Assert.assertTrue(result.containsKey("nonExisting0"));
      Assert.assertTrue(result.containsKey("nonExisting1"));
      Assert.assertTrue(result.containsKey("nonExisting2"));
      Assert.assertEquals(1, result.get("nonExisting0").getValueAsInteger());
      Assert.assertEquals(2, result.get("nonExisting1").getValueAsInteger());
      Assert.assertEquals(3, result.get("nonExisting2").getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRenameMultipleListElementsWithRegexGroupExpr() throws StageException {
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/listOfInts[(*)]";
    renameConfig1.toFieldExpression = "/nonExisting$1";
    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig1),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("listOfInts",
          Field.create(
              ImmutableList.of(
                  Field.create(Field.Type.INTEGER, 1),
                  Field.create(Field.Type.INTEGER, 2),
                  Field.create(Field.Type.INTEGER, 3)
              )
          )
      );
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Map<String, Field> result = output.getRecords().get("a").get(0).get().getValueAsMap();
      Assert.assertTrue(result.containsKey("listOfInts"));
      Assert.assertTrue(result.get("listOfInts").getValueAsList().isEmpty());
      Assert.assertTrue(result.containsKey("nonExisting0"));
      Assert.assertTrue(result.containsKey("nonExisting1"));
      Assert.assertTrue(result.containsKey("nonExisting2"));
      Assert.assertEquals(1, result.get("nonExisting0").getValueAsInteger());
      Assert.assertEquals(2, result.get("nonExisting1").getValueAsInteger());
      Assert.assertEquals(3, result.get("nonExisting2").getValueAsInteger());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRenameWithMultipleMapElementsExpr() throws StageException {
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/oe1/(*)";
    renameConfig1.toFieldExpression = "/oe1/irename_$1";

    FieldRenamerConfig renameConfig2 = new FieldRenamerConfig();
    renameConfig2.fromFieldExpression = "/oe2/(*)";
    renameConfig2.toFieldExpression = "/oe2/irename_$1";

    FieldRenamerConfig renameConfig3 = new FieldRenamerConfig();
    renameConfig3.fromFieldExpression = "/oe3/(*)";
    renameConfig3.toFieldExpression = "/oe3/irename_$1";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig1, renameConfig2, renameConfig3),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> innerMap = new HashMap<>();
      innerMap.put("ie1", Field.create("ie1"));
      innerMap.put("ie2", Field.create("ie2"));
      innerMap.put("ie3", Field.create("ie3"));


      Map<String, Field> map = new LinkedHashMap<>();
      map.put("oe1", Field.create(new HashMap<>(innerMap)));
      map.put("oe2", Field.create(new HashMap<>(innerMap)));
      map.put("oe3", Field.create(new HashMap<>(innerMap)));


      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Map<String, Field> result = output.getRecords().get("a").get(0).get().getValueAsMap();
      Assert.assertTrue(result.containsKey("oe1"));
      Assert.assertTrue(result.containsKey("oe2"));
      Assert.assertTrue(result.containsKey("oe3"));

      Map<String, Field> outerResult = result.get("oe1").getValueAsMap();
      Assert.assertTrue(outerResult.containsKey("irename_ie1"));
      Assert.assertTrue(outerResult.containsKey("irename_ie2"));
      Assert.assertTrue(outerResult.containsKey("irename_ie3"));
      Assert.assertEquals("ie1", outerResult.get("irename_ie1").getValueAsString());
      Assert.assertEquals("ie2", outerResult.get("irename_ie2").getValueAsString());
      Assert.assertEquals("ie3", outerResult.get("irename_ie3").getValueAsString());

      outerResult = result.get("oe2").getValueAsMap();
      Assert.assertTrue(outerResult.containsKey("irename_ie1"));
      Assert.assertTrue(outerResult.containsKey("irename_ie2"));
      Assert.assertTrue(outerResult.containsKey("irename_ie3"));
      Assert.assertEquals("ie1", outerResult.get("irename_ie1").getValueAsString());
      Assert.assertEquals("ie2", outerResult.get("irename_ie2").getValueAsString());
      Assert.assertEquals("ie3", outerResult.get("irename_ie3").getValueAsString());


      outerResult = result.get("oe3").getValueAsMap();
      Assert.assertTrue(outerResult.containsKey("irename_ie1"));
      Assert.assertTrue(outerResult.containsKey("irename_ie2"));
      Assert.assertTrue(outerResult.containsKey("irename_ie3"));
      Assert.assertEquals("ie1", outerResult.get("irename_ie1").getValueAsString());
      Assert.assertEquals("ie2", outerResult.get("irename_ie2").getValueAsString());
      Assert.assertEquals("ie3", outerResult.get("irename_ie3").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRenameOrderingComparator() throws StageException {
    List<String> fieldPaths = Arrays.asList(
        "/elementPrim",
        "/elementList1[2]",
        "/elementList1[1]",
        "/elementList1[0]",
        "/elementList2[0]",
        "/elementList2[1]",
        "/elementList2[2]",
        "/elementList[2]/elementMap/elementList[0]/elementPrim",
        "/elementList[1]/elementMap/elementList[2]/elementPrim",
        "/elementList[0]/elementMap/elementList[1]/elementPrim",
        "/elementList[1]/elementMap/elementList[1]/elementPrim",
        "/elementList[0]/elementMap/elementList[0]/elementPrim",
        "/elementList[2]/elementMap/elementList[2]/elementPrim",
        "/elementList[0]/elementMap/elementList[2]/elementPrim",
        "/elementList[2]/elementMap/elementList[1]/elementPrim",
        "/elementList[1]/elementMap/elementList[0]/elementPrim"
        );
    Collections.sort(fieldPaths, new FieldRenamerProcessor.FieldRenamerPathComparator());
    List<String> expectedOrder = Arrays.asList(
        "/elementPrim",
        "/elementList1[2]",
        "/elementList1[1]",
        "/elementList1[0]",
        "/elementList2[2]",
        "/elementList2[1]",
        "/elementList2[0]",
        "/elementList[2]/elementMap/elementList[2]/elementPrim",
        "/elementList[2]/elementMap/elementList[1]/elementPrim",
        "/elementList[2]/elementMap/elementList[0]/elementPrim",
        "/elementList[1]/elementMap/elementList[2]/elementPrim",
        "/elementList[1]/elementMap/elementList[1]/elementPrim",
        "/elementList[1]/elementMap/elementList[0]/elementPrim",
        "/elementList[0]/elementMap/elementList[2]/elementPrim",
        "/elementList[0]/elementMap/elementList[1]/elementPrim",
        "/elementList[0]/elementMap/elementList[0]/elementPrim"
        );

    for (int i =0 ;i < expectedOrder.size(); i++) {
      Assert.assertEquals(expectedOrder.get(i), fieldPaths.get(i));
    }
  }

  @Test
  public void testUnreachableFields() throws Exception {
    FieldRenamerConfig renameConfig = new FieldRenamerConfig();
    renameConfig.fromFieldExpression = "/a";
    renameConfig.toFieldExpression = "/b/c/d";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig),  errorHandler);

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").setOnRecordError(OnRecordError.TO_ERROR).build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a", Field.create(123));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));

      Assert.assertEquals(0, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Record errorRecord = runner.getErrorRecords().get(0);
      Assert.assertEquals(Errors.FIELD_RENAMER_04.name(), errorRecord.getHeader().getErrorCode());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSourceWithQuotedSubstring() throws StageException {
    // source should be processed as quoted string.
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/'attr|OrderNum'";
    renameConfig1.toFieldExpression = "/theOrderNum";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig1),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("attr|OrderNum", Field.create(Field.Type.STRING, "foo"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));
      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertFalse(result.containsKey("/'attr|OrderNum'"));
      Assert.assertFalse(result.containsKey("'attr|OrderNum'"));
      Assert.assertFalse(result.containsKey("attr|OrderNum"));
      Assert.assertTrue(result.containsKey("theOrderNum"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNameWithSlash() throws StageException {
    // source should be processed as quoted string.
    FieldRenamerConfig renameConfig1 = new FieldRenamerConfig();
    renameConfig1.fromFieldExpression = "/(.*)";
    renameConfig1.toFieldExpression = "/moved_$1";

    FieldRenamerProcessorErrorHandler errorHandler = new FieldRenamerProcessorErrorHandler();
    errorHandler.nonExistingFromFieldHandling = OnStagePreConditionFailure.CONTINUE;
    errorHandler.multipleFromFieldsMatching = OnStagePreConditionFailure.TO_ERROR;
    errorHandler.existingToFieldHandling = ExistingToFieldHandling.REPLACE;

    FieldRenamerProcessor processor =
        new FieldRenamerProcessor(ImmutableList.of(renameConfig1),  errorHandler);

    // Test non-existent source with existing target field
    ProcessorRunner runner = new ProcessorRunner.Builder(FieldRenamerDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      map.put("a/b", Field.create(Field.Type.STRING, "foo"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(1, output.getRecords().get("a").size());

      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);

      Map<String, Field> result = field.getValueAsMap();
      Assert.assertEquals(String.valueOf(result), 1, result.size());
      Assert.assertFalse(result.containsKey("/'a/b'"));

    } finally {
      runner.runDestroy();
    }
  }
}
