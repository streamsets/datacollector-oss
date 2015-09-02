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
package com.streamsets.pipeline.sdk.annotationsprocessor.json.test;

import com.streamsets.datacollector.json.ObjectMapperFactory;

import com.streamsets.pipeline.sdk.annotationsprocessor.PipelineAnnotationsProcessor;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TestUtil {

  private  static List<String> getGeneratedStageCollection() throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().
        getResourceAsStream(PipelineAnnotationsProcessor.STAGES_DEFINITION_RESOURCE);
    return getCollection(inputStream, "stageClasses");
  }

  private static List<String> getCollection(InputStream inputStream, String key) throws IOException {
    return (List)(ObjectMapperFactory.get().readValue(inputStream, Map.class).get(key));
  }

  public static void compareExpectedAndActualStages(String expectedJsonFileName) {
    try {
      List<String> actualStages = TestUtil.getGeneratedStageCollection();

      InputStream in = Thread.currentThread().getContextClassLoader().
          getResourceAsStream(expectedJsonFileName);

      List<String> expectedStages = TestUtil.getCollection(in, "stageClasses");

      Assert.assertEquals(new HashSet<>(expectedStages), new HashSet<>(actualStages));
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
  }


  public static List<String> getELDefsCollection() {
    try {
      InputStream inputStream = Thread.currentThread().getContextClassLoader().
          getResourceAsStream(PipelineAnnotationsProcessor.EL_DEFINITION_RESOURCE);
      return getCollection(inputStream, "elClasses");
    } catch (IOException ex) {
      Assert.fail(ex.toString());
      return null;
    }
  }

}
