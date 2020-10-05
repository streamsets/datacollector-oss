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

public class SdcServerInstanceInspector implements HealthInspector {

  private static final long GB = 1024*1024*1024;

  @Override
  public String getName() {
    return "Data Collector Process";
  }

  @Override
  public HealthInspectorResult inspectHealth(Context context) {
    HealthInspectorResult.Builder builder = new HealthInspectorResult.Builder(this);

    // Pure record count
    int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
    builder.addEntry("Number of threads", HealthInspectorEntry.Severity.smallerIsBetter(threadCount, 200, 500))
        .withValue(threadCount)
        .withDescription("Number of threads running inside Data Collector Java process.");

    // Deadlocked threads
    long []deadlockedThreadIds = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
    long deadlockedThreads = deadlockedThreadIds == null ? 0: deadlockedThreadIds.length;
    builder.addEntry("Deadlocked threads", deadlockedThreads == 0 ? HealthInspectorEntry.Severity.GREEN : HealthInspectorEntry.Severity.RED)
        .withValue(deadlockedThreads)
        .withDescription("Number of threads that are deadlocked (waiting on a resource they can't get).");

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
        .withDescription("How much percent of the max memory is currently allocated inside the JVM.");

    com.sun.management.OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    long maxSystemMemory = bean.getTotalPhysicalMemorySize();
    builder.addEntry("System Memory Max", HealthInspectorEntry.Severity.higherIsBetter(maxSystemMemory, 4*GB, 2*GB))
        .withValue(FileUtils.byteCountToDisplaySize(maxSystemMemory))
        .withDescription("Total memory available in the operating system.");

    long systemMemoryUtilization = (long)(((double)maxMemory / maxSystemMemory) * 100);
    builder.addEntry("System Memory Utilization", HealthInspectorEntry.Severity.smallerIsBetter(systemMemoryUtilization, 80, 90))
        .withValue(systemMemoryUtilization + " %")
        .withDescription("How much percent of OS memory can the JVM allocate.");


    return builder.build();
  }

}
