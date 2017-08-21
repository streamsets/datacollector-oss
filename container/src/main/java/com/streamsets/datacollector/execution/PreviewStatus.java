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

// When changing this class, make sure that you change PreviewInfoJson in CLI as well.
public enum PreviewStatus {
  CREATED(false),       // The preview was just created and nothing else has happened yet

  VALIDATING(true),     // validating the configuration, during preview
  VALID(false),          // configuration is valid, during preview
  INVALID(false),        // configuration is invalid, during preview
  VALIDATION_ERROR(false),   // validation failed with an exception, during validation

  STARTING(true),       // preview starting (initialization)
  START_ERROR(false),    // preview failed while start (during initialization)
  RUNNING(true),        // preview running
  RUN_ERROR(false),      // preview failed while running

  FINISHING(true),      // preview finishing (calling destroy on pipeline)
  FINISHED(false),       // preview finished  (done)

  CANCELLING(true),     // preview has been manually stopped
  CANCELLED(false),      // preview has been manually stopped

  TIMING_OUT(true),     //preview/validate time out
  TIMED_OUT(false),     //preview/validate time out
  ;

  private final boolean isActive;

  PreviewStatus(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean isActive() {
    return isActive;
  }
}
