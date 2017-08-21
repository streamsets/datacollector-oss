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
package com.streamsets.datacollector.memory;

import com.codahale.metrics.Counter;
import com.google.common.base.Supplier;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMonitor implements Runnable {
  private final Logger LOG = LoggerFactory.getLogger(MemoryMonitor.class);

  private final Counter memoryConsumed;
  private final Supplier<MemoryUsageCollector> memoryUsageCollector;

  public MemoryMonitor(Counter memoryConsumed, Supplier<MemoryUsageCollector> memoryUsageCollector) {
    this.memoryConsumed = memoryConsumed;
    this.memoryUsageCollector = memoryUsageCollector;
  }

  @Override
  public void run() {
    MemoryUsageSnapshot snapshot = memoryUsageCollector.get().collect();
    String name = snapshot.getStageName();
    LOG.debug(Utils.format("Stage {} consumed {} ({}ms), ClassLoader loaded {} classes",
      name, Utils.humanReadableInt(snapshot.getMemoryConsumed()), snapshot.getElapsedTime(),
      snapshot.getNumClassesLoaded()));
    long currentValue = memoryConsumed.getCount();
    memoryConsumed.inc((snapshot.getMemoryConsumed() / 1000000) - currentValue);
  }
}
