/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import java.io.Serializable;

public class MemoryLimitConfiguration implements Serializable {
  private MemoryLimitExceeded memoryLimitExceeded;
  private long memoryLimit;

  public MemoryLimitConfiguration() {
    this(MemoryLimitExceeded.STOP_PIPELINE, PipelineDefConfigs.MEMORY_LIMIT_DEFAULT);
  }

  public MemoryLimitConfiguration(MemoryLimitExceeded memoryLimitExceeded, long memoryLimit) {
    this.memoryLimitExceeded = memoryLimitExceeded;
    this.memoryLimit = memoryLimit;
  }

  public MemoryLimitExceeded getMemoryLimitExceeded() {
    return memoryLimitExceeded;
  }

  public void setMemoryLimitExceeded(MemoryLimitExceeded memoryLimitExceeded) {
    this.memoryLimitExceeded = memoryLimitExceeded;
  }

  /**
   * Returns the memory limit in MiB
   * @return
   */
  public long getMemoryLimit() {
    return memoryLimit;
  }

  /**
   * Set the memory limit in MiB
   * @param memoryLimit
   */
  public void setMemoryLimit(long memoryLimit) {
    this.memoryLimit = memoryLimit;
  }
}
