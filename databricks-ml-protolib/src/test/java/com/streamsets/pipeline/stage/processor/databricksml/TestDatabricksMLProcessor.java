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
package com.streamsets.pipeline.stage.processor.databricksml;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestDatabricksMLProcessor {

  private static String stringInputModelPath;
  private static String vectorInputModelPath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    ClassLoader classLoader = TestDatabricksMLProcessorBuilder.class.getClassLoader();
    stringInputModelPath = classLoader.getResource("20news_pipeline").getPath();
    vectorInputModelPath = classLoader.getResource("lr_pipeline").getPath();
  }

  @Test
  public void testInitConfig() throws StageException {
    Processor sparkMLProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(stringInputModelPath)
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, sparkMLProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testEmptyModelPath() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath("")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(DatabricksMLProcessorConfigBean.MODEL_PATH_CONFIG));
  }

  @Test
  public void testInvalidModelPath() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath("invalid")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(DatabricksMLProcessorConfigBean.MODEL_PATH_CONFIG));
  }

  @Test
  public void testProcessWithStringInput() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(stringInputModelPath)
        .outputColumns(ImmutableList.of("label", "prediction", "probability"))
        .inputField("/")
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("topic", Field.create("sci.space"));
    field.put("text", Field.create("NASA sent a rocket to outer space with scientific ice cream."));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, output.getRecords().get("a").size());

    Field outputField = output.getRecords().get("a").get(0).get("/output");
    Assert.assertNotNull(outputField);
    Assert.assertEquals(Field.Type.MAP, outputField.getType());

    Map<String, Field> outputValue = outputField.getValueAsMap();
    Assert.assertTrue(outputValue.containsKey("label"));
    Assert.assertTrue(outputValue.containsKey("prediction"));
    Assert.assertTrue(outputValue.containsKey("probability"));

    Field labelField = output.getRecords().get("a").get(0).get("/output/label");
    Assert.assertNotNull(labelField);
    Assert.assertEquals(7.0, labelField.getValueAsDouble(), 0.0);
  }

  @Test
  public void testProcessInvalidFieldPath() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(stringInputModelPath)
        .outputColumns(ImmutableList.of("label", "prediction", "probability"))
        .inputField("/invalidPath")
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("topic", Field.create("sci.space"));
    field.put("text", Field.create("NASA sent a rocket to outer space with scientific ice cream."));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    String errorCode = runner.getErrorRecords().get(0).getHeader().getErrorCode();
    Assert.assertEquals(errorCode, Errors.DATABRICKS_ML_03.getCode());
  }

  @Test
  public void testProcessInvalidFieldValue() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(stringInputModelPath)
        .outputColumns(ImmutableList.of("label", "prediction", "probability"))
        .inputField("/")
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    record.set(Field.create(Field.Type.STRING, "invalid JSON String"));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    String errorCode = runner.getErrorRecords().get(0).getHeader().getErrorCode();
    Assert.assertEquals(errorCode, Errors.DATABRICKS_ML_04.getCode());
  }

  @Test
  public void testProcessInvalidOutputColumns() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(stringInputModelPath)
        .outputColumns(ImmutableList.of("invalidColumn"))
        .inputField("/")
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("topic", Field.create("sci.space"));
    field.put("text", Field.create("NASA sent a rocket to outer space with scientific ice cream."));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, runner.getErrorRecords().size());
    String errorCode = runner.getErrorRecords().get(0).getHeader().getErrorCode();
    Assert.assertEquals(errorCode, Errors.DATABRICKS_ML_04.getCode());
    String errorMessage = runner.getErrorRecords().get(0).getHeader().getErrorMessage();
    Assert.assertTrue(errorMessage.contains("Missing fields"));
  }


  @Test
  public void testProcessWithVectorInput() throws StageException {
    Processor tensorFlowProcessor  = new TestDatabricksMLProcessorBuilder()
        .modelPath(vectorInputModelPath)
        .outputColumns(ImmutableList.of("label", "prediction", "probability"))
        .inputField("/")
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(DatabricksMLDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("origLabel", Field.create(-1.0));

    LinkedHashMap<String, Field> featuresField = new LinkedHashMap<>();
    featuresField.put("type", Field.create(0));
    featuresField.put("size", Field.create(13));
    featuresField.put("indices", Field.create(ImmutableList.of(
        Field.create(0),
        Field.create(2),
        Field.create(3),
        Field.create(4),
        Field.create(6),
        Field.create(7),
        Field.create(8),
        Field.create(9),
        Field.create(10),
        Field.create(11),
        Field.create(12)
    )));
    featuresField.put("values", Field.create(ImmutableList.of(
        Field.create(74.0),
        Field.create(2.0),
        Field.create(120.0),
        Field.create(269.0),
        Field.create(2.0),
        Field.create(121.0),
        Field.create(1.0),
        Field.create(0.2),
        Field.create(1.0),
        Field.create(1.0),
        Field.create(2.0)
    )));
    field.put("features", Field.createListMap(featuresField));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, output.getRecords().get("a").size());

    Field outputField = output.getRecords().get("a").get(0).get("/output");
    Assert.assertNotNull(outputField);
    Assert.assertEquals(Field.Type.MAP, outputField.getType());

    Map<String, Field> outputValue = outputField.getValueAsMap();
    Assert.assertTrue(outputValue.containsKey("label"));
    Assert.assertTrue(outputValue.containsKey("prediction"));
    Assert.assertTrue(outputValue.containsKey("probability"));

    Field labelField = output.getRecords().get("a").get(0).get("/output/label");
    Assert.assertNotNull(labelField);
    Assert.assertEquals(0.0, labelField.getValueAsDouble(), 0.0);
  }

}
