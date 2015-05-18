/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spark;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.pipeline.stage.origin.spark.EmbeddedSDC;
import com.streamsets.pipeline.stage.origin.spark.EmbeddedSDCPool;

public class TestEmbeddedSDCPool {

  static class DummyEmbeddedSDCPool extends EmbeddedSDCPool {
    public DummyEmbeddedSDCPool(Properties properties, String pipelineJson) throws Exception {
      super(properties, pipelineJson);
    }
    @Override
    protected EmbeddedSDC createEmbeddedSDC() throws Exception {
      return new EmbeddedSDC();
    }
  }

  @Test
  public void testEmbeddedSDC() throws Exception {
    DummyEmbeddedSDCPool dummyEmbeddedSDCPool = new DummyEmbeddedSDCPool(new Properties(), "");
    Assert.assertEquals(1, dummyEmbeddedSDCPool.getTotalInstances().size());
    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());
    // Now retrieve the created one
    EmbeddedSDC embeddedSDC1 = dummyEmbeddedSDCPool.getEmbeddedSDC();
    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());
    Assert.assertEquals(1, dummyEmbeddedSDCPool.getTotalInstances().size());

    // This should create new SDC
    EmbeddedSDC embeddedSDC2 = dummyEmbeddedSDCPool.getEmbeddedSDC();
    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());
    Assert.assertEquals(2, dummyEmbeddedSDCPool.getTotalInstances().size());

    // Return one back
    dummyEmbeddedSDCPool.returnEmbeddedSDC(embeddedSDC1);
    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());

    dummyEmbeddedSDCPool.returnEmbeddedSDC(embeddedSDC2);
    Assert.assertEquals(2, dummyEmbeddedSDCPool.size());
    Assert.assertEquals(2, dummyEmbeddedSDCPool.getTotalInstances().size());

    // Return the same instance back again
    dummyEmbeddedSDCPool.returnEmbeddedSDC(embeddedSDC1);
    Assert.assertEquals(2, dummyEmbeddedSDCPool.size());
  }
}
