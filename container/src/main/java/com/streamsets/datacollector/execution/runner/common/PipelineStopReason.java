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
package com.streamsets.datacollector.execution.runner.common;

/**
 * Reason why pipeline was stopped (moved to "not running" state).
 */
public enum PipelineStopReason {
  // We don't know why the pipeline stopped
  UNKNOWN,

  // When this property semantically doesn't make sense (e.g. pipeline has not been running in the first place)
  UNUSED,

  // The pipeline was stopped on user action (e.g. for example user pressed STOP button)
  USER_ACTION,

  // The pipeline failed - either in init() phase or during execution
  FAILURE,

  // The pipeline finished properly - e.g. the Source.produce() returned true or PushSource.produce returned.
  FINISHED,
}
