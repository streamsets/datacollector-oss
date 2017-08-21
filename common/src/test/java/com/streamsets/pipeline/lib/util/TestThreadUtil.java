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
