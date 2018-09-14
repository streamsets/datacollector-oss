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
package com.streamsets.pipeline.stage.processor.mleap;

import com.google.common.collect.ImmutableList;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestMLeapProcessor {

  private static String airbnbRFModelZipFilePath;

  @BeforeClass
  public static void beforeTest() throws Exception {
    ClassLoader classLoader = TestMLeapProcessorBuilder.class.getClassLoader();
    airbnbRFModelZipFilePath = classLoader.getResource("airbnb.model.rf.zip").getPath();
  }

  @Test
  public void testInitConfig() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .inputFieldConfig("security_deposit", "/security_deposit")
        .inputFieldConfig("bedrooms", "/bedrooms")
        .inputFieldConfig("instant_bookable", "/instant_bookable")
        .inputFieldConfig("room_type", "/room_type")
        .inputFieldConfig("state", "/state")
        .inputFieldConfig("cancellation_policy", "/cancellation_policy")
        .inputFieldConfig("square_feet", "/square_feet")
        .inputFieldConfig("number_of_reviews", "/number_of_reviews")
        .inputFieldConfig("extra_people", "/extra_people")
        .inputFieldConfig("bathrooms", "/bathrooms")
        .inputFieldConfig("host_is_superhost", "/host_is_superhost")
        .inputFieldConfig("review_scores_rating", "/review_scores_rating")
        .inputFieldConfig("cleaning_fee", "/cleaning_fee")
        .outputFieldNames(ImmutableList.of("price_prediction"))
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testInvalidModelPath() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath("invalid")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(MLeapProcessorConfigBean.MODEL_PATH_CONFIG));
  }

  @Test
  public void testProcess() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .inputFieldConfig("security_deposit", "/security_deposit")
        .inputFieldConfig("bedrooms", "/bedrooms")
        .inputFieldConfig("instant_bookable", "/instant_bookable")
        .inputFieldConfig("room_type", "/room_type")
        .inputFieldConfig("state", "/state")
        .inputFieldConfig("cancellation_policy", "/cancellation_policy")
        .inputFieldConfig("square_feet", "/square_feet")
        .inputFieldConfig("number_of_reviews", "/number_of_reviews")
        .inputFieldConfig("extra_people", "/extra_people")
        .inputFieldConfig("bathrooms", "/bathrooms")
        .inputFieldConfig("host_is_superhost", "/host_is_superhost")
        .inputFieldConfig("review_scores_rating", "/review_scores_rating")
        .inputFieldConfig("cleaning_fee", "/cleaning_fee")
        .outputFieldNames(ImmutableList.of("price_prediction"))
        .outputField("/output")
        .build();

    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> field = new LinkedHashMap<>();
    field.put("security_deposit", Field.create(50.0));
    field.put("bedrooms", Field.create(3.0));
    field.put("instant_bookable", Field.create("1.0"));
    field.put("room_type", Field.create("Entire home/apt"));
    field.put("state", Field.create("NY"));
    field.put("cancellation_policy", Field.create("strict"));
    field.put("square_feet", Field.create(1250.0));
    field.put("number_of_reviews", Field.create(56.0));
    field.put("extra_people", Field.create(2.0));
    field.put("bathrooms", Field.create(2.0));
    field.put("host_is_superhost", Field.create("1.0"));
    field.put("review_scores_rating", Field.create(90.0));
    field.put("cleaning_fee", Field.create(30.0));
    record.set(Field.createListMap(field));

    StageRunner.Output output = runner.runProcess(ImmutableList.of(record, record));
    Assert.assertEquals(2, output.getRecords().get("a").size());

    for (Record outputRecord: output.getRecords().get("a")) {
      Field outputField = outputRecord.get("/output");
      Assert.assertNotNull(outputField);
      Assert.assertEquals(Field.Type.LIST_MAP, outputField.getType());

      Map<String, Field> outputValue = outputField.getValueAsMap();
      Assert.assertTrue(outputValue.containsKey("price_prediction"));

      Field rateField = outputRecord.get("/output/price_prediction");
      Assert.assertNotNull(rateField);
      Assert.assertEquals(218.2767196535019, rateField.getValueAsDouble(), 0.0);
    }
  }


  @Test
  public void testInvalidInputField() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .outputFieldNames(ImmutableList.of("price_prediction"))
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(MLeapProcessorConfigBean.INPUT_CONFIGS_CONFIG));

    // Provide all the required Input field
    mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .inputFieldConfig("security_deposit", "/security_deposit")
        .inputFieldConfig("bedrooms", "/bedrooms")
        .inputFieldConfig("instant_bookable", "/instant_bookable")
        .inputFieldConfig("room_type", "/room_type")
        .inputFieldConfig("state", "/state")
        .inputFieldConfig("cancellation_policy", "/cancellation_policy")
        .inputFieldConfig("square_feet", "/square_feet")
        .inputFieldConfig("number_of_reviews", "/number_of_reviews")
        .inputFieldConfig("extra_people", "/extra_people")
        .inputFieldConfig("bathrooms", "/bathrooms")
        .inputFieldConfig("host_is_superhost", "/host_is_superhost")
        .inputFieldConfig("review_scores_rating", "/review_scores_rating")
        .inputFieldConfig("cleaning_fee", "/cleaning_fee")
        .outputFieldNames(ImmutableList.of("price_prediction"))
        .build();
    runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }



  @Test
  public void testEmptyOutputField() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .inputFieldConfig("security_deposit", "/security_deposit")
        .inputFieldConfig("bedrooms", "/bedrooms")
        .inputFieldConfig("instant_bookable", "/instant_bookable")
        .inputFieldConfig("room_type", "/room_type")
        .inputFieldConfig("state", "/state")
        .inputFieldConfig("cancellation_policy", "/cancellation_policy")
        .inputFieldConfig("square_feet", "/square_feet")
        .inputFieldConfig("number_of_reviews", "/number_of_reviews")
        .inputFieldConfig("extra_people", "/extra_people")
        .inputFieldConfig("bathrooms", "/bathrooms")
        .inputFieldConfig("host_is_superhost", "/host_is_superhost")
        .inputFieldConfig("review_scores_rating", "/review_scores_rating")
        .inputFieldConfig("cleaning_fee", "/cleaning_fee")
        .outputField("/output")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(MLeapProcessorConfigBean.OUTPUT_FIELD_NAMES_CONFIG));
  }

  @Test
  public void testInvalidOutputField() throws StageException {
    Processor mLeapProcessor  = new TestMLeapProcessorBuilder()
        .modelPath(airbnbRFModelZipFilePath)
        .inputFieldConfig("security_deposit", "/security_deposit")
        .inputFieldConfig("bedrooms", "/bedrooms")
        .inputFieldConfig("instant_bookable", "/instant_bookable")
        .inputFieldConfig("room_type", "/room_type")
        .inputFieldConfig("state", "/state")
        .inputFieldConfig("cancellation_policy", "/cancellation_policy")
        .inputFieldConfig("square_feet", "/square_feet")
        .inputFieldConfig("number_of_reviews", "/number_of_reviews")
        .inputFieldConfig("extra_people", "/extra_people")
        .inputFieldConfig("bathrooms", "/bathrooms")
        .inputFieldConfig("host_is_superhost", "/host_is_superhost")
        .inputFieldConfig("review_scores_rating", "/review_scores_rating")
        .inputFieldConfig("cleaning_fee", "/cleaning_fee")
        .outputFieldNames(ImmutableList.of(
            "price_prediction",
            "InvalidOutputFieldName1",
            "InvalidOutputFieldName2"
        ))
        .outputField("/output")
        .build();
    ProcessorRunner runner = new ProcessorRunner.Builder(MLeapDProcessor.class, mLeapProcessor)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(MLeapProcessorConfigBean.OUTPUT_FIELD_NAMES_CONFIG));
    Assert.assertTrue(issues.get(0).toString().contains("InvalidOutputFieldName1"));
    Assert.assertTrue(issues.get(0).toString().contains("InvalidOutputFieldName2"));
  }
}
