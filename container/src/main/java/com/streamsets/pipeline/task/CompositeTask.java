/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.task;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.container.Utils;

import java.util.ArrayList;
import java.util.List;

public class CompositeTask extends AbstractTask {
  private final List<Task> subTasks;
  private int initedTaskIndex;

  public CompositeTask(String name, List<Task> subTasks) {
    super(name);
    this.subTasks = ImmutableList.copyOf(subTasks);
  }

  @Override
  protected void initTask() {
    for (initedTaskIndex = 0; initedTaskIndex < subTasks.size(); initedTaskIndex++) {
      subTasks.get(initedTaskIndex).init();
    }
  }

  @Override
  protected void runTask() {
    for (Task subTask : subTasks) {
      subTask.run();
    }
  }

  @Override
  protected void stopTask() {
    for (initedTaskIndex--; initedTaskIndex >= 0; initedTaskIndex--) {
      subTasks.get(initedTaskIndex).stop();
    }
  }

  @Override
  public void waitWhileRunning() throws InterruptedException {
    for (Task subTask : subTasks) {
      subTask.waitWhileRunning();
    }
  }

  @Override
  public String toString() {
    List<String> names = new ArrayList<String>(subTasks.size());
    for (Task subTask : subTasks) {
      names.add(subTask.getName());
    }
    return Utils.format("{}[subTasks='{}' status='{}']", getName(), names, getStatus());
  }

}
