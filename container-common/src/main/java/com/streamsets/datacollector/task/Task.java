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
package com.streamsets.datacollector.task;

/**
 * A Task is a component with a lifecycle that provides a service to the system.
 * <p/>
 * The lifecycle of a task is <code>init()</code> -> <code>run()</code> -> <code>stop()</code>.
 * <p/>
 * The <code>run()</code> must not block, it must return once the task is up and running.
 */
public interface Task {

  /**
   * Possible status of a task.
   */
  public enum Status { CREATED, INITIALIZED, RUNNING, STOPPED, ERROR }

  /**
   * Returns the task name.
   *
   * @return the task name;
   */
  public String getName();

  /**
   * Initializes the task.
   *
   * @throws RuntimeException thrown if there is an initialization problem, the caller should not call {@link #run()} or
   *                          {@link #stop()} if an exception is thrown during initialization.
   */
  public void init();

  /**
   * Runs the task.
   *
   * @throws RuntimeException thrown if there is a start up problem, the caller should not call {@link #run()} or
   *                          {@link #stop()} if an exception is thrown during initialization.
   */
  public void run();

  /**
   * Blocks the current thread while the task is running.
   *
   * @throws InterruptedException thrown if the thread was interrupted while waiting.
   */
  public void waitWhileRunning() throws InterruptedException;

  /**
   * Stops the task.
   * <p/>
   * This method should not thrown any {@link RuntimeException}.
   */
  public void stop();

  /**
   * Returns the current status of the task.
   *
   * @return the current status of the task, it never returns <code>null</code>.
   */
  public Status getStatus();
}
