/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


public class TestThreadUtil {

  @Test
  public void testSleepNotInterrupted() {
    Assert.assertTrue(ThreadUtil.sleep(1));
  }

  @Test
  public void testSleepInterrupted() {
    final CountDownLatch latch = new CountDownLatch(1);
    final Thread t = Thread.currentThread();
    new Thread() {

      @Override
      public void run() {
        try {
          latch.await();
          Thread.sleep(100);
        } catch (InterruptedException ex) {
        }
        t.interrupt();
      }
    }.start();
    // using the latch to try to get the thread interrupted once it is sleeping, though not guarantees.
    latch.countDown();
    Assert.assertFalse(ThreadUtil.sleep(10000));
  }

  @Test
  public void testSleepPreInterrupted() {
    final Thread t = Thread.currentThread();
    t.interrupt();
    Assert.assertFalse(ThreadUtil.sleep(10000));
  }

}
