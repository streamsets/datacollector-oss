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

public class MemoryUsageSnapshot {
  private final Object stage;
  private final ClassLoader classLoader;
  private long memoryConsumedByInstances;
  private long memoryConsumedByClasses;
  private long elapsedTimeByInstances;
  private long elapsedTimeByClasses;
  private int numClassesLoaded;

  public MemoryUsageSnapshot(Object stage, ClassLoader classLoader) {
    this.stage = stage;
    this.classLoader = classLoader;
    this.memoryConsumedByInstances = 0;
    this.memoryConsumedByClasses = 0;
    this.elapsedTimeByInstances = 0;
    this.elapsedTimeByClasses = 0;
    this.numClassesLoaded = 0;
  }

  public String getStageName() {
    return String.valueOf(stage);
  }

  public long getMemoryConsumed() {
    return memoryConsumedByInstances + memoryConsumedByClasses;
  }

  public MemoryUsageSnapshot addMemoryConsumedByInstances(long memoryConsumed) {
    this.memoryConsumedByInstances += memoryConsumed;
    return this;
  }

  public MemoryUsageSnapshot addMemoryConsumedByClasses(long memoryConsumed) {
    this.memoryConsumedByClasses += memoryConsumed;
    return this;
  }

  public long getElapsedTime() {
    return elapsedTimeByInstances + elapsedTimeByClasses;
  }

  public MemoryUsageSnapshot addElapsedTimeByInstances(long elapsedTime) {
    this.elapsedTimeByInstances += elapsedTime;
    return this;
  }

  public MemoryUsageSnapshot addElapsedTimeByClasses(long elapsedTime) {
    this.elapsedTimeByClasses += elapsedTime;
    return this;
  }

  public int getNumClassesLoaded() {
    return numClassesLoaded;
  }

  public void setNumClassesLoaded(int numClassesLoaded) {
    this.numClassesLoaded = numClassesLoaded;
  }

  @Override
  public String toString() {
    return "MemoryUsageSnapshot{" +
      "stage=" + (stage == null ? null : stage.getClass().getName())+ // object to string could be large
      ", classLoader=" + classLoader +
      ", memoryConsumedByInstances=" + memoryConsumedByInstances +
      ", memoryConsumedByClasses=" + memoryConsumedByClasses +
      ", elapsedTimeByInstances=" + elapsedTimeByInstances +
      ", elapsedTimeByClasses=" + elapsedTimeByClasses +
      ", numClassesLoaded=" + numClassesLoaded +
      '}';
  }
}
