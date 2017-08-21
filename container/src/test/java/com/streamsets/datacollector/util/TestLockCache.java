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
package com.streamsets.datacollector.util;

import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.util.LockCache;

public class TestLockCache {
  @Test
  public void testLocker() {
    LockCache<String> lockCache = new LockCache<String>();
    Object obj1 = lockCache.getLock("L1");
    Object obj2 = lockCache.getLock("L2");
    assertTrue(obj1 != obj2);
    Object obj3 = lockCache.getLock("L1");
    assertTrue(obj1 == obj3);
  }

  @Test
  public void testSameLock() throws Exception {
    StringBuffer sb = new StringBuffer("");
    LockCache<String> lockCache = new LockCache<String>();
    Locker l1 = new Locker("a", 1, lockCache, "L1", 1000, sb);
    Locker l2 = new Locker("a", 2, lockCache, "L1", 1000, sb);
    new Thread(l1).start();
    // sleep to make sure l1 gets the lock first
    Thread.sleep(500);
    // L2 should not get lock till l1 releases
    new Thread(l2).start();
    while(!l1.isDone() || !l2.isDone()) {
      Thread.sleep(100);
    }
    Assert.assertEquals("a:1-B a:1-L a:2-B a:1-R a:2-L a:2-R", sb.toString().trim());
  }

  @Test
  public void testDifferentLocks() throws Exception {
    StringBuffer sb = new StringBuffer("");
    LockCache<String> lockCache = new LockCache<String>();
    Locker l1 = new Locker("a", 1, lockCache, "L1", 1000, sb);
    Locker l2 = new Locker("a", 2, lockCache, "L2", 1500, sb);
    new Thread(l1).start();
    // Sleep for time less than l1's lock timeout
    Thread.sleep(500);
    // L2 should also get lock
    new Thread(l2).start();
    // Wait for this to finish
    while(!l1.isDone() || !l2.isDone()) {
      Thread.sleep(100);
    }
    Assert.assertEquals("a:1-B a:1-L a:2-B a:2-L a:1-R a:2-R", sb.toString().trim());

  }

  class Locker implements Runnable {
    protected String name;
    private final String nameIndex;
    private final StringBuffer sb;
    private final LockCache<String> lockCache;
    private final String resource;
    private final long timeout;
    private volatile boolean done;

    public Locker(String name, int nameIndex, LockCache<String> lockCache, String lockResource,
        long timeout, StringBuffer buffer) {
      this.name = name;
      this.nameIndex = name + ":" + nameIndex;
      this.sb = buffer;
      this.lockCache = lockCache;
      this.resource = lockResource;
      this.timeout = timeout;
      done = false;
    }

    @Override
    public void run() {
      try {
        sb.append(nameIndex + "-B ");
        synchronized (lockCache.getLock(resource)) {
          sb.append(nameIndex + "-L ");
          Thread.sleep(timeout);
          sb.append(nameIndex + "-R ");
        }
        done = true;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public boolean isDone() {
      return done;
    }
  }

}

