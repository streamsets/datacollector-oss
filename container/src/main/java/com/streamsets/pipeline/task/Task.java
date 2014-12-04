/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

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
