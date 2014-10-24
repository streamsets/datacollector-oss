package com.streamsets.pipeline.sdk.test;

import com.streamsets.pipeline.sdk.SerializationUtil;
import com.streamsets.pipeline.sdk.StageCollection;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static com.streamsets.pipeline.sdk.Constants.PIPELINE_STAGES_JSON;
import static junit.framework.TestCase.fail;

/**
 * Created by harikiran on 10/24/14.
 */
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

  public static void testActualAndExpectedPipelineStagesJson(String expectedJsonFileName) {
    StageCollection actualStageCollection = TestUtil.getGeneratedStageCollection();

    InputStream in = null;
    try {
      in = new FileInputStream(expectedJsonFileName);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      Assert.fail("Test failed for the following reason:");
      e.printStackTrace();
    }
    StageCollection expectedtageCollection = TestUtil.getStageCollection(in);

    //check the deserialized StageCollections.
    Assert.assertTrue(actualStageCollection.getStageConfigurations().size() == 1);
    Assert.assertEquals(expectedtageCollection.getStageConfigurations().get(0).getStageOptions(),
      actualStageCollection.getStageConfigurations().get(0).getStageOptions());
    Assert.assertEquals(expectedtageCollection.getStageConfigurations().get(0).getConfigOptions(),
      actualStageCollection.getStageConfigurations().get(0).getConfigOptions());
  }
}
