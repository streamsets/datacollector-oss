/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor.json.test;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.pipeline.sdk.annotationsprocessor.Constants;

import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TestUtil {

  public static List<String> getGeneratedStageCollection() throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().
        getResourceAsStream(Constants.PIPELINE_STAGES_JSON);
    return getStageCollection(inputStream);
  }

  public static List<String> getStageCollection(InputStream inputStream) throws IOException {
    return (List)(ObjectMapperFactory.get().readValue(inputStream, Map.class).get("stageClasses"));
  }

  public static void compareExpectedAndActualStages(String expectedJsonFileName) {
    try {
      List<String> actualStages = TestUtil.getGeneratedStageCollection();

      InputStream in = Thread.currentThread().getContextClassLoader().
          getResourceAsStream(expectedJsonFileName);

      List<String> expectedStages = TestUtil.getStageCollection(in);

      Assert.assertEquals(new HashSet<String>(expectedStages), new HashSet<String>(actualStages));
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
  }

}
