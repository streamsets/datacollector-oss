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
package com.streamsets.datacollector.execution;

public enum PipelineStatus {
  EDITED (false),          // pipeline job has been create/modified, didn't run since the creation/modification

  STARTING (true),         // pipeline job starting (initialization)
  STARTING_ERROR(true),     // Pipeline failed while starting (but the destroy did not finished yet)
  START_ERROR (false),      // pipeline job failed while start (during initialization) or failed while submission in cluster mode

  RUNNING (true),          // pipeline job running
  RUNNING_ERROR (true),    // pipeline job failed while running (calling destroy on pipeline) - only for standalone
  RUN_ERROR (false),        // pipeline job failed while running (done)

  RETRY (true),           // Retry - only for standalone

  FINISHING (true),        // pipeline job finishing (source reached end, returning NULL offset) (calling destroy on pipeline) - only for standalone
  FINISHED (false),         // pipeline job finished                                              (done)

  KILLED (false),           // only happens in cluster mode


  STOPPING (true),         // pipeline job has been manually stopped (calling destroy on pipeline)
  STOPPED (false),          // pipeline job has been manually stopped (done)

  STOPPING_ERROR(true),    // There was a problem when stopping pipeline
  STOP_ERROR(false),       // Terminal state representing that pipeline failed to stop properly

  DISCONNECTING (true),    // SDC going down gracefully (calling destroy on pipeline for LOCAL, doing nothing for CLUSTER)
  DISCONNECTED (true),     // SDC going down gracefully (done)

  CONNECTING (true),       // SDC starting back (transition to STARTING for LOCAL, for CLUSTER checks job still running)
                    //                   (and transitions to RUNNING or RUN_ERROR -streaming- or FINISHED -batch)
  CONNECT_ERROR (true),     // failed to get to RUNNING, on SDC restart will retry again - only for cluster mode

  DELETED(false)
  ;

  private final boolean isActive;

  PipelineStatus(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean isActive() {
    return isActive;
  }

  public boolean isOneOf(PipelineStatus ...statuses) {
    if(statuses == null) {
      return false;
    }

    for(PipelineStatus s : statuses) {
      if(this == s) {
        return true;
      }
    }

    return false;
  }

}
