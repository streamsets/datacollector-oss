/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.streamsets.pipeline.EmbeddedSDC;
import com.streamsets.pipeline.EmbeddedSDCPool;

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

  @Test(timeout = 60000)
  public void testEmbeddedSDC() throws Exception {
    Properties props = new Properties();
    props.setProperty("sdc.pool.size.infinite", "true");
    DummyEmbeddedSDCPool dummyEmbeddedSDCPool = new DummyEmbeddedSDCPool(props, "");
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

  @Test(timeout = 90000)
  public void testEmbeddedSDCTimeout() throws Exception {
    Properties props = new Properties();
    DummyEmbeddedSDCPool dummyEmbeddedSDCPool = new DummyEmbeddedSDCPool(props, "");
    Assert.assertEquals(1, dummyEmbeddedSDCPool.getTotalInstances().size());
    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());
    EmbeddedSDC embeddedSDC1 = dummyEmbeddedSDCPool.getEmbeddedSDC();
    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());

    // Nothing in pool, so wait() should return null
    assertNull(dummyEmbeddedSDCPool.waitForSDC(3000));
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));

    WaitOnSDCRunnable th = new WaitOnSDCRunnable(dummyEmbeddedSDCPool);
    ListenableFuture future = executorService.submit(th);
    Thread.sleep(100);
    // shouldn't be done yet
    assertTrue(!future.isDone());
    assertNull(th.embeddedSDC);
    // return back sdc to pool
    dummyEmbeddedSDCPool.returnEmbeddedSDC(embeddedSDC1);
    assertNull(future.get());
    assertEquals(embeddedSDC1, th.embeddedSDC);
  }

  private class WaitOnSDCRunnable implements Runnable {
    private EmbeddedSDCPool embeddedPool;
    public EmbeddedSDC embeddedSDC;

    public WaitOnSDCRunnable(EmbeddedSDCPool embeddedPool) {
      this.embeddedPool = embeddedPool;
    }

    @Override
    public void run() {
      try {
        doWaiting();
      } catch (Exception e) {
        throw new RuntimeException("Failed wait", e);
      }
    }

    void doWaiting() throws Exception {
      embeddedSDC = embeddedPool.getEmbeddedSDC();
    }
  }
}
