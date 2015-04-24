/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestProdSourceOffsetTracker {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testProductionSourceOffsetTracker() {

    RuntimeInfo info = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(getClass().getClassLoader()));
    ProductionSourceOffsetTracker offsetTracker = new ProductionSourceOffsetTracker(PIPELINE_NAME, PIPELINE_REV, info);

    Assert.assertEquals(false, offsetTracker.isFinished());
    Assert.assertEquals(null, offsetTracker.getOffset());

    offsetTracker.setOffset("abc");
    offsetTracker.commitOffset();
    Assert.assertEquals("abc", offsetTracker.getOffset());

    Assert.assertEquals(OffsetFileUtil.getPipelineOffsetFile(info, PIPELINE_NAME, PIPELINE_REV).lastModified(),
      offsetTracker.getLastBatchTime());
  }


}
