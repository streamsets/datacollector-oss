/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.main.RuntimeInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class TestProdSourceOffsetTracker {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testProductionSourceOffsetTracker() {

    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(PIPELINE_NAME, PIPELINE_REV, info);

    Assert.assertEquals(false, offsetTracker.isFinished());
    Assert.assertEquals(null, offsetTracker.getOffset());

    offsetTracker.setOffset("abc");
    offsetTracker.commitOffset();
    Assert.assertEquals("abc", offsetTracker.getOffset());
  }
}
