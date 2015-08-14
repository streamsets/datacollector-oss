/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class MultipleMonitorLocker {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageCollector.class);
  private static final Unsafe UNSAFE;
  static {
    Unsafe unsafe = null;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe)unsafeField.get(null);
    } catch (Exception e) {
      String msg = "Could not find Unsafe: " + e;
      LOG.error(msg, e);
    }
    UNSAFE = unsafe;
  }

  public static boolean isEnabled() {
    return UNSAFE != null;
  }

  public static <RESULT> RESULT lock(List<Object> locks, Callable<RESULT> callable)
  throws Exception {
    if (!isEnabled()) {
      return null;
    }
    List<Object> locked = new ArrayList<>();
    try {
      for (Object lock : locks) {
        if (UNSAFE.tryMonitorEnter(lock)) {
          locked.add(lock);
        }
      }
      if (locks.size() == locked.size()) {
        return callable.call();
      }
      return null;
    } finally {
      for (Object lock : locked) {
        UNSAFE.monitorExit(lock);
      }
    }
  }
}
