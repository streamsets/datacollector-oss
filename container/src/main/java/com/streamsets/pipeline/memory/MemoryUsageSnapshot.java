/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.memory;

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