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
package com.streamsets.datacollector.main;

import com.streamsets.datacollector.task.Task;
import org.slf4j.Logger;

public class ShutdownHandler implements Runnable {

  private final Logger finalLog;
  private final Task task;
  private final ShutdownStatus shutdownStatus;

  public ShutdownHandler(Logger finalLog, Task task, ShutdownStatus shutdownStatus) {
    this.finalLog = finalLog;
    this.task = task;
    this.shutdownStatus = shutdownStatus;
  }

  @Override
  public void run() {
    finalLog.debug("Stopping, reason: requested");
    task.stop();
  }

  void setExistStatus(int status) {
    this.shutdownStatus.setExitStatus(status);
  }

  public static class ShutdownStatus {
    private int exitStatus = 0;

    public int getExitStatus() {
      return exitStatus;
    }

    public void setExitStatus(int exitStatus) {
      this.exitStatus = exitStatus;
    }
  }
}
