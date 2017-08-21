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
