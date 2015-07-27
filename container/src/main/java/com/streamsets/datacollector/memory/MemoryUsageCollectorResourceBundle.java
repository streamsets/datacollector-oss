/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.memory;


import java.util.ArrayDeque;
import java.util.Deque;

public class MemoryUsageCollectorResourceBundle {

  private static final int INITIAL_CAPACITY = 1000;
  private final Deque stack = new ArrayDeque(INITIAL_CAPACITY);
  private final IntOpenHashSet objectSet = new IntOpenHashSet(1000);

  public Deque getStack() {
    return stack;
  }

  public IntOpenHashSet getObjectSet() {
    return objectSet;
  }
}
