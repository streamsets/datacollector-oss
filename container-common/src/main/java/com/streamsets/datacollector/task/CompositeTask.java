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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CompositeTask extends AbstractTask {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeTask.class);

  private final List<Task> subTasks;
  private int initedTaskIndex;
  private final boolean monitorSubTasksStatus;
  private Thread monitorThread;

  public CompositeTask(String name, List<Task> subTasks, boolean monitorSubTasksStatus) {
    super(name);
    this.subTasks = ImmutableList.copyOf(subTasks);
    this.monitorSubTasksStatus = monitorSubTasksStatus;
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
    if (monitorSubTasksStatus) {

      //TODO convert this monitoring to wait/interrupt pattern

      LOG.debug("'{}' creating subTasks status monitor thread", getName());
      monitorThread = new Thread(Utils.format("CompositeTask '{}' monitor thread", getName())) {
        @Override
        public void run() {
          while (getStatus() == Status.RUNNING) {
            for (Task subTask : subTasks) {
              if (subTask.getStatus() != Status.RUNNING) {
                if (getStatus() == Status.RUNNING) {
                  LOG.warn("'{}' status monitor thread detected that subTask '{}' is not running anymore, stopping",
                           getName(), subTask.getName());
                  CompositeTask.this.stopTask();
                  CompositeTask.this.stop();
                }
              }
            }
            // ignoring interrupts, we want to keep monitoring subtasks until they are all done.
            ThreadUtil.sleep(50);
          }
        }
      };
      monitorThread.setDaemon(true);
      monitorThread.start();
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
    List<String> names = new ArrayList<>(subTasks.size());
    for (Task subTask : subTasks) {
      names.add(subTask.getName());
    }
    return Utils.format("{}[subTasks='{}' status='{}']", getName(), names, getStatus());
  }

}
