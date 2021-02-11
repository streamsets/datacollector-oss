/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.datacollector.bundles.content;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class BundleGeneratorUtils {

  /**
   * Create Thread Dump (e.g. get list of threads and associated info).
   * @return
   */
  public static ThreadInfo[] getThreadInfos() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    return threadMXBean.dumpAllThreads(true, true);
  }

  /**
   * Convert stack traces (thread dump) into String in almost the standard way.
   *
   * @param threads Threads as retried by getThreadInfos()
   * @return String representation
   */
  public static String threadInfosToString(ThreadInfo[] threads) {
    StringBuilder sb = new StringBuilder();

    // Sadly we can't easily do info.toString() as the implementation is hardcoded to cut the stack trace only to 8
    // items which does not serve our purpose well. Hence we have custom implementation that prints entire stack trace
    // for all threads.
    for(ThreadInfo info: threads) {
      sb.append("\"" + info.getThreadName() + "\"" + " Id=" + info.getThreadId() + " " + info.getThreadState());
      if (info.getLockName() != null) {
        sb.append(" on " + info.getLockName());
      }
      if (info.getLockOwnerName() != null) {
        sb.append(" owned by \"" + info.getLockOwnerName() + "\" Id=" + info.getLockOwnerId());
      }
      if (info.isSuspended()) {
        sb.append(" (suspended)");
      }
      if (info.isInNative()) {
        sb.append(" (in native)");
      }
      sb.append('\n');
      int i = 0;
      for(StackTraceElement ste : info.getStackTrace()) {
        if (i == 0 && info.getLockInfo() != null) {
          Thread.State ts = info.getThreadState();
          switch (ts) {
            case BLOCKED:
              sb.append("\t-  blocked on " + info.getLockInfo());
              sb.append('\n');
              break;
            case WAITING:
              sb.append("\t-  waiting on " + info.getLockInfo());
              sb.append('\n');
              break;
            case TIMED_WAITING:
              sb.append("\t-  waiting on " + info.getLockInfo());
              sb.append('\n');
              break;
            default:
          }
        }
        sb.append("\tat " + ste.toString());
        sb.append('\n');

        i++;

        for (MonitorInfo mi : info.getLockedMonitors()) {
          if (mi.getLockedStackDepth() == i) {
            sb.append("\t-  locked " + mi);
            sb.append('\n');
          }
        }
      }

      LockInfo[] locks = info.getLockedSynchronizers();
      if (locks.length > 0) {
        sb.append("\n\tNumber of locked synchronizers = " + locks.length);
        sb.append('\n');
        for (LockInfo li : locks) {
          sb.append("\t- " + li);
          sb.append('\n');
        }
      }
      sb.append('\n');
    }

    return sb.toString();
  }

  private BundleGeneratorUtils() {
    // Instantiation is prohibited.
  }
}
