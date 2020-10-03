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

import java.lang.management.ManagementFactory;

public class SdcServerInstanceInspector implements HealthInspector {
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

    return builder.build();
  }

}
