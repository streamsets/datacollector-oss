/**
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
package com.streamsets.datacollector.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@SuppressWarnings("deprecation")
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

  private MultipleMonitorLocker() {}

  public static boolean isEnabled() {
    return UNSAFE != null;
  }

  public static <RESULT> RESULT lock(List<Object> locks, Callable<RESULT> callable) throws Exception {
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
