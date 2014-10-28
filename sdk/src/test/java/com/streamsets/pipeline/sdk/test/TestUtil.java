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
package com.streamsets.pipeline.sdk.test;

import com.streamsets.pipeline.sdk.SerializationUtil;
import com.streamsets.pipeline.sdk.StageCollection;
import com.streamsets.pipeline.sdk.StageConfiguration;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static com.streamsets.pipeline.sdk.Constants.PIPELINE_STAGES_JSON;
import static junit.framework.TestCase.fail;

public class TestUtil {

  public static StageCollection getGeneratedStageCollection() {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().
      getResourceAsStream(PIPELINE_STAGES_JSON);
    return getStageCollection(inputStream);
  }

  public static StageCollection getStageCollection(InputStream inputStream) {
    StageCollection stageCollection = null;
    try {
      stageCollection = SerializationUtil.deserialize(inputStream);
    } catch (IOException e) {
      fail("Failed during deserialing the generated PipelineStages.json file. Reason : " + e.getMessage());
      e.printStackTrace();
    }
    return stageCollection;
  }

  public static void compareExpectedAndActualStages(String expectedJsonFileName) {
    StageCollection actualStages = TestUtil.getGeneratedStageCollection();

    InputStream in = null;
    try {
      in = new FileInputStream(expectedJsonFileName);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      Assert.fail("Test failed for the following reason:");
      e.printStackTrace();
    }
    StageCollection expectedStages = TestUtil.getStageCollection(in);

    Assert.assertTrue(actualStages.getStageConfigurations().size() ==
      expectedStages.getStageConfigurations().size());
    //check the deserialized StageCollections.
    for(int i = 0; i < actualStages.getStageConfigurations().size(); i++) {
      StageConfiguration expected = expectedStages.getStageConfigurations().get(i);
      StageConfiguration actual = null;
      for(StageConfiguration s : actualStages.getStageConfigurations()) {
        if(s.getStageOptions().get("name").equals(expected.getStageOptions().get("name"))) {
          actual = s;
          break;
        }
      }
      if(actual == null) {
        Assert.fail("A Stage configuration with name " +
          expected.getStageOptions().get("name") +
          "is expected, but not found.");
      }
      Assert.assertEquals(expected.getStageOptions(), actual.getStageOptions());
      Assert.assertEquals(expected.getConfigOptions(), actual.getConfigOptions());
    }
  }
}
