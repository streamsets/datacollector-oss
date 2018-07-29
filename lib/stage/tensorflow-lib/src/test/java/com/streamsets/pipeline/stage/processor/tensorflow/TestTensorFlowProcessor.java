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
package com.streamsets.pipeline.stage.processor.tensorflow;

import com.streamsets.pipeline.api.Field;
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
import org.tensorflow.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestTensorFlowProcessor {

  private static String irisModelPath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    ClassLoader classLoader = TestTensorFlowProcessorBuilder.class.getClassLoader();
    irisModelPath = classLoader.getResource("iris_saved_model").getPath();
  }

  @Test
  public void testInitConfig() throws StageException {
    Processor tensorFlowProcessor  = new TestTensorFlowProcessorBuilder()
        .modelPath(irisModelPath)
        .modelTags(Collections.singletonList("serve"))
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(TensorFlowDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testEmptyModelPath() throws StageException {
    Processor tensorFlowProcessor  = new TestTensorFlowProcessorBuilder()
        .modelPath("")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(TensorFlowDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(TensorFlowConfigBean.MODEL_PATH_CONFIG));
  }

  @Test
  public void testInvalidModelPath() throws StageException {
    Processor tensorFlowProcessor  = new TestTensorFlowProcessorBuilder()
        .modelPath("invalid")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(TensorFlowDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(TensorFlowConfigBean.MODEL_PATH_CONFIG));
  }


  @Test
  public void testProcess() throws StageException {
    Processor tensorFlowProcessor  = new TestTensorFlowProcessorBuilder()
        .modelPath(irisModelPath)
        .modelTags(Collections.singletonList("serve"))
        .useEntireBatch(false)
        .addInputConfigs(
            "PetalLength",
            0,
            Collections.singletonList("/petalLength"),
            Collections.singletonList(1),
            DataType.FLOAT
        )
        .addInputConfigs(
            "PetalWidth",
            0,
            Collections.singletonList("/petalWidth"),
            Collections.singletonList(1),
            DataType.FLOAT
        )
        .addInputConfigs(
            "SepalLength",
            0,
            Collections.singletonList("/sepalLength"),
            Collections.singletonList(1),
            DataType.FLOAT
        )
        .addInputConfigs(
            "SepalWidth",
            0,
            Collections.singletonList("/sepalWidth"),
            Collections.singletonList(1),
            DataType.FLOAT
        )
        .addOutputConfigs("dnn/head/predictions/ExpandDims",0, DataType.FLOAT)
        .addOutputConfigs("dnn/head/predictions/probabilities",0, DataType.FLOAT)
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(TensorFlowDProcessor.class, tensorFlowProcessor)
        .addOutputLane("a")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("petalLength", Field.create(6.4f));
    field.put("petalWidth", Field.create(2.8f));
    field.put("sepalLength", Field.create(5.6f));
    field.put("sepalWidth", Field.create(2.2f));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(Collections.singletonList(record));
    Assert.assertEquals(1, output.getRecords().get("a").size());

    Field outputField = output.getRecords().get("a").get(0).get("/output");
    Assert.assertNotNull(outputField);
    Assert.assertEquals(Field.Type.MAP, outputField.getType());

    Map<String, Field> outputValue = outputField.getValueAsMap();
    Assert.assertTrue(outputValue.containsKey("dnn/head/predictions/ExpandDims_0"));
    Assert.assertTrue(outputValue.containsKey("dnn/head/predictions/probabilities_0"));

    Field expandDimsField = output.getRecords().get("a").get(0).get("/output/'dnn/head/predictions/ExpandDims_0'[0]");
    Assert.assertNotNull(expandDimsField);
    Assert.assertEquals(2, expandDimsField.getValueAsLong());
  }

}
