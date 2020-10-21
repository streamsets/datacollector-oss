/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.inspector.inspectors;

import com.streamsets.datacollector.inspector.HealthInspector;
import com.streamsets.datacollector.inspector.model.HealthInspectorResult;
import com.streamsets.datacollector.inspector.model.HealthInspectorEntry;
import org.apache.commons.io.FileUtils;

import java.lang.management.ManagementFactory;

public class JvmInstanceInspector implements HealthInspector {

  private static final long GB = 1024*1024*1024;
  private static final String GC1_YOUNG = "G1 Young Generation";
  private static final String GC1_OLD = "G1 Old Generation";
  private static final String CMS_PARNEW = "ParNew";
  private static final String CMS_CMS = "ConcurrentMarkSweep";

  @Override
  public String getName() {
    return "Java Virtual Machine (JVM) Process";
  }

  @Override
  public HealthInspectorResult inspectHealth(Context context) {
    HealthInspectorResult.Builder builder = new HealthInspectorResult.Builder(this);

    // Pure record count
    int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
    builder.addEntry("Thread count", HealthInspectorEntry.Severity.smallerIsBetter(threadCount, 200, 500))
        .withValue(threadCount)
        .withDescription("Current count of threads running inside Data Collector Java process.");

    // Deadlocked threads
    long []deadlockedThreadIds = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
    long deadlockedThreads = deadlockedThreadIds == null ? 0: deadlockedThreadIds.length;
    builder.addEntry("Deadlocked threads", deadlockedThreads == 0 ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.RED)
        .withValue(deadlockedThreads)
        .withDescription("Number of threads that are deadlocked (waiting on a resource they can't get) as reported by ManagementFactory.getThreadMXBean().");

    // Various memory related information
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    builder.addEntry("JVM Memory Max", HealthInspectorEntry.Severity.higherIsBetter(maxMemory, 3*GB, 1*GB))
        .withValue(FileUtils.byteCountToDisplaySize(maxMemory))
        .withDescription("Total memory that JVM can allocate as returned by Runtime.getRuntime() - which will differ from -Xmx argument on command line.");

    long freeMemory = runtime.freeMemory();
    long memoryUtilization = (long)((double)(maxMemory - freeMemory)/maxMemory*100);
    builder.addEntry("JVM Memory Utilization", HealthInspectorEntry.Severity.smallerIsBetter(memoryUtilization, 80, 90))
        .withValue(memoryUtilization +" %")
        .withDescription("How much percent of memory is allocated and used out of the 'JVM Memory Max'.");

    com.sun.management.OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    long maxSystemMemory = bean.getTotalPhysicalMemorySize();
    builder.addEntry("System Memory Max", HealthInspectorEntry.Severity.higherIsBetter(maxSystemMemory, 4*GB, 2*GB))
        .withValue(FileUtils.byteCountToDisplaySize(maxSystemMemory))
        .withDescription("Total memory available in the operating system.");

    long systemMemoryUtilization = (long)(((double)maxMemory / maxSystemMemory) * 100);
    builder.addEntry("System Memory Utilization", HealthInspectorEntry.Severity.smallerIsBetter(systemMemoryUtilization, 80, 90))
        .withValue(systemMemoryUtilization + " %")
        .withDescription("How much percent of OS memory can the JVM allocate.");


    for(java.lang.management.GarbageCollectorMXBean gcBean: ManagementFactory.getGarbageCollectorMXBeans()) {
      if(gcBean instanceof com.sun.management.GarbageCollectorMXBean) {
        // Casting to internal stuff
        com.sun.management.GarbageCollectorMXBean internal = (com.sun.management.GarbageCollectorMXBean)gcBean;
        com.sun.management.GcInfo gcInfo = internal.getLastGcInfo();

        // Now the different cases that we are explicitly testing
        if(GC1_YOUNG.equals(gcBean.getName())) {
          builder.addEntry("Garbage Collection: Young Gen", gcInfo == null ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.smallerIsBetter(gcInfo.getDuration(), 100, 300))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for GC1 Yong Generation");
        }
        if(GC1_OLD.equals(gcBean.getName())) {
          builder.addEntry("Garbage Collection: Old Gen", gcInfo == null ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.smallerIsBetter(gcInfo.getDuration(), 1000, 3000))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for GC1 Old Generation");
        }
        if(CMS_PARNEW.equals(gcBean.getName())) {
          builder.addEntry("Garbage Collection: ParNew", gcInfo == null ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.smallerIsBetter(gcInfo.getDuration(), 100, 300))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for ParNew");
        }
        if(CMS_CMS.equals(gcBean.getName())) {
          builder.addEntry("Garbage Collection: CMS", gcInfo == null ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.smallerIsBetter(gcInfo.getDuration(), 20_000, 60_000))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for Concurrent Mark & Sweep Phase");
        }
      }
    }


    return builder.build();
  }

}
