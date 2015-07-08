/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.memory;

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

  public void run() {
    MemoryUsageSnapshot snapshot = memoryUsageCollector.get().collect();
    String name = snapshot.getStageName();
    LOG.debug(Utils.format("Stage {} consumed {} ({}ms), ClassLoader loaded {} classes",
      name, Utils.humanReadableInt(snapshot.getMemoryConsumed()), snapshot.getElapsedTime(),
      snapshot.getNumClassesLoaded()));
    long currentValue = memoryConsumed.getCount();
    memoryConsumed.inc(snapshot.getMemoryConsumed() - currentValue);
  }
}
