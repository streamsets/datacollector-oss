/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.task;

import javax.inject.Inject;

public class TaskWrapper implements Task {
  private Task task;

  @Inject
  public TaskWrapper(Task task) {
    this.task = task;
  }

  @Override
  public String getName() {
    return task.getName();
  }

  @Override
  public Status getStatus() {
    return task.getStatus();
  }

  @Override
  public void init() {
    task.init();
  }

  @Override
  public void run() {
    task.run();
  }

  @Override
  public void stop() {
    task.stop();
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    task.waitWhileRunning();
  }
  public Task getTask() {
    return task;
  }
}
