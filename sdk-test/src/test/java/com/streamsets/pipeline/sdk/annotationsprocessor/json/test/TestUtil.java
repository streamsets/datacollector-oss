/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
