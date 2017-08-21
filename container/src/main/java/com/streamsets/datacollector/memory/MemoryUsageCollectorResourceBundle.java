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


import com.carrotsearch.hppc.IntHashSet;

import java.util.ArrayDeque;
import java.util.Deque;

public class MemoryUsageCollectorResourceBundle {

  private static final int INITIAL_CAPACITY = 1000;
  private final Deque stack = new ArrayDeque(INITIAL_CAPACITY);
  private final IntHashSet objectSet = new IntHashSet(INITIAL_CAPACITY);

  public Deque getStack() {
    return stack;
  }

  public IntHashSet getObjectSet() {
    return objectSet;
  }
}
