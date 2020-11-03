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
package com.streamsets.datacollector.inspector.categories;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.inspector.HealthCategory;
import com.streamsets.datacollector.inspector.model.HealthCategoryResult;
import com.streamsets.datacollector.inspector.model.HealthCheck;
import com.streamsets.datacollector.util.ProcessUtil;
import org.apache.commons.io.FileUtils;

import java.lang.management.ManagementFactory;

public class JvmInstanceHealthCategory implements HealthCategory {

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
  public HealthCategoryResult inspectHealth(Context context) {
    HealthCategoryResult.Builder builder = new HealthCategoryResult.Builder(this);

    // Pure record count
    int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
    builder.addHealthCheck("Thread count", HealthCheck.Severity.smallerIsBetter(threadCount, 200, 500))
        .withValue(threadCount)
        .withDescription("Current count of threads running inside Data Collector Java process.");

    // Deadlocked threads
    long []deadlockedThreadIds = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
    long deadlockedThreads = deadlockedThreadIds == null ? 0: deadlockedThreadIds.length;
    builder.addHealthCheck("Deadlocked threads", deadlockedThreads == 0 ? HealthCheck.Severity.GREEN : HealthCheck.Severity.RED)
        .withValue(deadlockedThreads)
        .withDescription("Number of threads that are deadlocked (waiting on a resource they can't get) as reported by ManagementFactory.getThreadMXBean().");

    // Various memory related information
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    builder.addHealthCheck("JVM Memory Max", HealthCheck.Severity.higherIsBetter(maxMemory, 3*GB, 1*GB))
        .withValue(FileUtils.byteCountToDisplaySize(maxMemory))
        .withDescription("Total memory that JVM can allocate as returned by Runtime.getRuntime() - which will differ from -Xmx argument on command line.");

    long freeMemory = runtime.freeMemory();
    long memoryUtilization = (long)((double)(maxMemory - freeMemory)/maxMemory*100);
    builder.addHealthCheck("JVM Memory Utilization", HealthCheck.Severity.smallerIsBetter(memoryUtilization, 80, 90))
        .withValue(memoryUtilization +" %")
        .withDescription("How much percent of memory is allocated and used out of the 'JVM Memory Max'.");

    com.sun.management.OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    long maxSystemMemory = bean.getTotalPhysicalMemorySize();
    builder.addHealthCheck("System Memory Max", HealthCheck.Severity.higherIsBetter(maxSystemMemory, 4*GB, 2*GB))
        .withValue(FileUtils.byteCountToDisplaySize(maxSystemMemory))
        .withDescription("Total memory available in the operating system.");

    long systemMemoryUtilization = (long)(((double)maxMemory / maxSystemMemory) * 100);
    builder.addHealthCheck("System Memory Utilization", HealthCheck.Severity.smallerIsBetter(systemMemoryUtilization, 80, 90))
        .withValue(systemMemoryUtilization + " %")
        .withDescription("How much percent of OS memory can the JVM allocate.");

    // GC Stats
    for(java.lang.management.GarbageCollectorMXBean gcBean: ManagementFactory.getGarbageCollectorMXBeans()) {
      if(gcBean instanceof com.sun.management.GarbageCollectorMXBean) {
        // Casting to internal stuff
        com.sun.management.GarbageCollectorMXBean internal = (com.sun.management.GarbageCollectorMXBean)gcBean;
        com.sun.management.GcInfo gcInfo = internal.getLastGcInfo();

        // Now the different cases that we are explicitly testing
        if(GC1_YOUNG.equals(gcBean.getName())) {
          builder.addHealthCheck("Garbage Collection: Young Gen", gcInfo == null ? HealthCheck.Severity.GREEN : HealthCheck.Severity.smallerIsBetter(gcInfo.getDuration(), 100, 300))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for GC1 Yong Generation");
        }
        if(GC1_OLD.equals(gcBean.getName())) {
          builder.addHealthCheck("Garbage Collection: Old Gen", gcInfo == null ? HealthCheck.Severity.GREEN : HealthCheck.Severity.smallerIsBetter(gcInfo.getDuration(), 1000, 3000))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for GC1 Old Generation");
        }
        if(CMS_PARNEW.equals(gcBean.getName())) {
          builder.addHealthCheck("Garbage Collection: ParNew", gcInfo == null ? HealthCheck.Severity.GREEN : HealthCheck.Severity.smallerIsBetter(gcInfo.getDuration(), 100, 300))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for ParNew");
        }
        if(CMS_CMS.equals(gcBean.getName())) {
          builder.addHealthCheck("Garbage Collection: CMS", gcInfo == null ? HealthCheck.Severity.GREEN : HealthCheck.Severity.smallerIsBetter(gcInfo.getDuration(), 20_000, 60_000))
              .withValue(gcInfo == null ? null : gcInfo.getDuration() + "ms")
              .withDescription("Number of millisecond spent in the last GC pause for Concurrent Mark & Sweep Phase");
        }
      }
    }

    // Get our own PID
    String pid = getPid();

    // Number of child processes
    ProcessUtil.Output childProcessesOutput = ProcessUtil.executeCommandAndLoadOutput(ImmutableList.of("pstree", pid), 5);
    int subProcesses = -1;
    if(childProcessesOutput.success && childProcessesOutput.stdout != null) {
      subProcesses = childProcessesOutput.stdout.split("\n").length;
      subProcesses--; // Removing line for our own process
    }
    builder.addHealthCheck("Child Processes", subProcesses == -1 ? HealthCheck.Severity.RED : HealthCheck.Severity.smallerIsBetter(subProcesses, 5, 10))
        .withValue(subProcesses == -1 ? null : subProcesses)
        .withDescription("Number of child processes created by this instance of Data Collector")
        .withDetails(childProcessesOutput);


    return builder.build();
  }

  /**
   * Return PID of our process in JDK 8 compatible manner.
   */
  private static String getPid() {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    if(name != null && name.contains("@")) {
      name = name.substring(0, name.indexOf('@'));
    }

    return name;
  }

}
