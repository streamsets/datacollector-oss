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
package com.streamsets.datacollector.execution.common;

public class ExecutorConstants {

  public static final String PREVIEWER_THREAD_POOL_SIZE_KEY = "previewer.thread.pool.size";
  public static final int PREVIEWER_THREAD_POOL_SIZE_DEFAULT = 4;

  public static final String RUNNER_THREAD_POOL_SIZE_KEY = "runner.thread.pool.size";
  public static final int RUNNER_THREAD_POOL_SIZE_DEFAULT = 50;
  public static final int RUNNER_THREAD_POOL_SIZE_MULTIPLIER = 10;

  public static final String RUNNER_STOP_THREAD_POOL_SIZE_KEY = "runner_stop.thread.pool.size";
  public static final String RUNNER_STOP_THREAD_POOL_KEEP_ALIVE_TIME_KEY = "runner_stop.thread.pool.size";
  public static final int RUNNER_STOP_THREAD_POOL_KEEP_ALIVE_TIME_DEFAULT = 5;

  public static final String MANAGER_EXECUTOR_THREAD_POOL_SIZE_KEY = "manager.executor.thread.pool.size";
  public static final int MANAGER_EXECUTOR_THREAD_POOL_SIZE_DEFAULT = 4;

  public static final String EVENT_EXECUTOR_THREAD_POOL_SIZE_KEY = "event.executor.thread.pool.size";
  public static final int EVENT_EXECUTOR_THREAD_POOL_SIZE_DEFAULT = 2;

  public static final String BUNDLE_EXECUTOR_THREAD_POOL_SIZE_KEY = "bundle.executor.thread.pool.size";
  public static final int BUNDLE_EXECUTOR_THREAD_POOL_SIZE_DEFAULT = 1;

  private ExecutorConstants() {}
}
