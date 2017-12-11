/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package com.streamsets.pipeline;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//
//import java.util.Properties;
//import java.util.concurrent.Executors;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import com.google.common.util.concurrent.ListenableFuture;
//import com.google.common.util.concurrent.ListeningExecutorService;
//import com.google.common.util.concurrent.MoreExecutors;
//
//public class TestEmbeddedSDCPool {
//
//  static class DummyEmbeddedSDCPool extends EmbeddedSDCPool {
//    public DummyEmbeddedSDCPool(Properties properties) throws Exception {
//      super(properties);
//    }
//
//    @Override
//    protected EmbeddedSDC create() throws Exception {
//      return new EmbeddedSDC();
//    }
//  }
//
//  @Test(timeout = 60000)
//  public void testEmbeddedSDC() throws Exception {
//    Properties props = new Properties();
//    props.setProperty("sdc.pool.size.infinite", "true");
//    DummyEmbeddedSDCPool dummyEmbeddedSDCPool = new DummyEmbeddedSDCPool(props);
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.getInstances().size());
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());
//    // Now retrieve the created one
//    EmbeddedSDC embeddedSDC1 = dummyEmbeddedSDCPool.checkout();
//    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.getInstances().size());
//
//    // This should create new SDC
//    EmbeddedSDC embeddedSDC2 = dummyEmbeddedSDCPool.checkout();
//    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());
//    Assert.assertEquals(2, dummyEmbeddedSDCPool.getInstances().size());
//
//    // Return one back
//    dummyEmbeddedSDCPool.checkin(embeddedSDC1);
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());
//
//    dummyEmbeddedSDCPool.checkin(embeddedSDC2);
//    Assert.assertEquals(2, dummyEmbeddedSDCPool.size());
//    Assert.assertEquals(2, dummyEmbeddedSDCPool.getInstances().size());
//
//    // Return the same instance back again
//    dummyEmbeddedSDCPool.checkin(embeddedSDC1);
//    Assert.assertEquals(2, dummyEmbeddedSDCPool.size());
//  }
//
//  @Test(timeout = 90000)
//  public void testEmbeddedSDCTimeout() throws Exception {
//    Properties props = new Properties();
//    DummyEmbeddedSDCPool dummyEmbeddedSDCPool = new DummyEmbeddedSDCPool(props);
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.getInstances().size());
//    Assert.assertEquals(1, dummyEmbeddedSDCPool.size());
//    EmbeddedSDC embeddedSDC1 = dummyEmbeddedSDCPool.checkout();
//    Assert.assertEquals(0, dummyEmbeddedSDCPool.size());
//
//    // Nothing in pool, so wait() should return null
//    assertNull(dummyEmbeddedSDCPool.waitForSDC(3000));
//    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
//
//    WaitOnSDCRunnable th = new WaitOnSDCRunnable(dummyEmbeddedSDCPool);
//    ListenableFuture future = executorService.submit(th);
//    Thread.sleep(100);
//    // shouldn't be done yet
//    assertTrue(!future.isDone());
//    assertNull(th.embeddedSDC);
//    // return back sdc to pool
//    dummyEmbeddedSDCPool.checkin(embeddedSDC1);
//    assertNull(future.get());
//    assertEquals(embeddedSDC1, th.embeddedSDC);
//  }
//
//  private class WaitOnSDCRunnable implements Runnable {
//    private EmbeddedSDCPool embeddedPool;
//    public EmbeddedSDC embeddedSDC;
//
//    public WaitOnSDCRunnable(EmbeddedSDCPool embeddedPool) {
//      this.embeddedPool = embeddedPool;
//    }
//
//    @Override
//    public void run() {
//      try {
//        doWaiting();
//      } catch (Exception e) {
//        throw new RuntimeException("Failed wait", e);
//      }
//    }
//
//    void doWaiting() throws Exception {
//      embeddedSDC = embeddedPool.checkout();
//    }
//  }
//}
