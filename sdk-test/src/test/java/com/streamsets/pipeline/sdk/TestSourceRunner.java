/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.junit.Assert;
import org.junit.Test;

public class TestSourceRunner {

  public static class DummySource extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      Assert.assertEquals("lastOffset", lastSourceOffset);
      Assert.assertEquals(10, maxBatchSize);
      batchMaker.addRecord(getContext().createRecord("a"));
      return "newOffset";
    }
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderNoOutput() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(stage);
    builder.build();
  }

  @Test
  public void testInitProduceDestroy() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runProduce("lastOffset", 10);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidProduce1() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runProduce("lastOffset", 10);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidProduce2() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runProduce("lastOffset", 10);
  }

  @Test
  public void testProduce() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce("lastOffset", 10);
      Assert.assertNotNull(output);
      Assert.assertEquals("newOffset", output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().size());
      Assert.assertTrue(output.getRecords().containsKey("a"));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertNotNull(output.getRecords().get("a").get(0));
    } finally {
      runner.runDestroy();
    }
  }

}
