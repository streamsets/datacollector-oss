/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.common;

public class ExecutorConstants {

  public static final String PREVIEWER_THREAD_POOL_SIZE_KEY = "previewer.thread.pool.size";
  public static final int PREVIEWER_THREAD_POOL_SIZE_DEFAULT = 4;
  public static final String RUNNER_THREAD_POOL_SIZE_KEY = "runner.thread.pool.size";
  public static final int RUNNER_THREAD_POOL_SIZE_DEFAULT = 20;
  public static final String ASYNC_EXECUTOR_THREAD_POOL_SIZE_KEY = "async.executor.thread.pool.size";
  public static final int ASYNC_EXECUTOR_THREAD_POOL_SIZE_DEFAULT = 4;
  public static final String MANAGER_EXECUTOR_THREAD_POOL_SIZE_KEY = "manager.executor.thread.pool.size";
  public static final int MANAGER_EXECUTOR_THREAD_POOL_SIZE_DEFAULT = 4;
}
