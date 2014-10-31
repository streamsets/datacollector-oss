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
package com.streamsets.pipeline.sdk.json.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.sdk.annotationsprocessor.Constants.PIPELINE_STAGES_JSON;

public class TestUtil {

  public static List<StageDefinition> getGeneratedStageCollection() {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().
      getResourceAsStream(PIPELINE_STAGES_JSON);
    return getStageCollection(inputStream);
  }

  public static List<StageDefinition> getStageCollection(InputStream inputStream) {
    ObjectMapper json = new ObjectMapper();
    List<StageDefinition> stageDefinitions = new ArrayList<StageDefinition>();
    try {
      StageDefinition[] stageDefArray = json.readValue(inputStream, StageDefinition[].class);
      for(StageDefinition s : stageDefArray) {
        stageDefinitions.add(s);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stageDefinitions;
  }

  public static void compareExpectedAndActualStages(String expectedJsonFileName) {
    List<StageDefinition> actualStages = TestUtil.getGeneratedStageCollection();

    InputStream in = null;
    try {
      in = new FileInputStream(expectedJsonFileName);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      Assert.fail("Test failed for the following reason:");
      e.printStackTrace();
    }
    List<StageDefinition> expectedStages = TestUtil.getStageCollection(in);

    Assert.assertTrue(actualStages.size() == expectedStages.size());
    //check the deserialized StageCollections.
    deepCompareStageDefinitions(expectedStages, actualStages);
  }

  public static void deepCompareStageDefinitions(
    List<StageDefinition> expectedStages, List<StageDefinition> actualStages) {
    for(int i = 0; i < actualStages.size(); i++) {
      StageDefinition expected = expectedStages.get(i);
      StageDefinition actual = null;
      for(StageDefinition s : actualStages) {
        if(s.getName().equals(expected.getName())) {
          actual = s;
          break;
        }
      }
      if(actual == null) {
        Assert.fail("A Stage configuration with name " +
          expected.getName() +
          "is expected, but not found.");
      }

      //compare stage definition properties
      Assert.assertEquals(expected.getName(), actual.getName());
      Assert.assertEquals(expected.getVersion(), actual.getVersion());
      Assert.assertEquals(expected.getLabel(), actual.getLabel());
      Assert.assertEquals(expected.getDescription(), actual.getDescription());
      Assert.assertEquals(expected.getType(), actual.getType());
      Assert.assertEquals(expected.getClassLoader(), actual.getClassLoader());
      Assert.assertEquals(expected.getClassName(), actual.getClassName());
      Assert.assertEquals(expected.getLibrary(), actual.getLibrary());
      Assert.assertEquals(expected.getStageClass(), actual.getStageClass());
      Assert.assertEquals(expected.getConfigDefinitions().size(),
        actual.getConfigDefinitions().size());
      //compare the config definitions
      for(int j = 0; i < expected.getConfigDefinitions().size(); i++) {
        ConfigDefinition e = expected.getConfigDefinitions().get(j);
        ConfigDefinition a = actual.getConfigDefinitions().get(j);

        Assert.assertEquals(e.getName(), a.getName());
        Assert.assertEquals(e.getDefaultValue(), a.getDefaultValue());
        Assert.assertEquals(e.getDescription(), a.getDescription());
        Assert.assertEquals(e.getLabel(), a.getLabel());
        Assert.assertEquals(e.getType(), a.getType());
        Assert.assertEquals(e.getFieldName(), a.getFieldName());
        Assert.assertEquals(e.isRequired(), a.isRequired());
        Assert.assertEquals(e.getGroup(), a.getGroup());

        if(e.getModel() != null) {
          Assert.assertNotNull(a.getModel());
          Assert.assertEquals(e.getModel().getFieldModifierType(), a.getModel().getFieldModifierType());
          Assert.assertEquals(e.getModel().getLabels(), a.getModel().getLabels());
          Assert.assertEquals(e.getModel().getModelType(), a.getModel().getModelType());
          Assert.assertEquals(e.getModel().getValues(), a.getModel().getValues());
        }

      }

    }
  }
}
