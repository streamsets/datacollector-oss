/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.manager.standalone;

public enum ThreadUsage {

  STANDALONE(22), //2.2 * ExecutorConstants.RUNNER_THREAD_POOL_SIZE_MULTIPLIER
  CLUSTER(2), //0.2 * ExecutorConstants.RUNNER_THREAD_POOL_SIZE_MULTIPLIER
  SLAVE(22); //2.2 * ExecutorConstants.RUNNER_THREAD_POOL_SIZE_MULTIPLIER

  private int resourceCount;

  ThreadUsage(int resourceCount) {
    this.resourceCount = resourceCount;
  }

  public int reserve(int currentCapacity) {
    return currentCapacity - resourceCount;
  }

  public int release(int currentCapacity) {
    return currentCapacity + resourceCount;
  }
}
