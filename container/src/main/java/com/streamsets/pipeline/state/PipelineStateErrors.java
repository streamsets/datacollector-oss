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
package com.streamsets.pipeline.state;

import com.streamsets.pipeline.api.ErrorId;

public enum PipelineStateErrors implements ErrorId {

  COULD_NOT_SET_STATE("Could not set state, {}"),
  COULD_NOT_GET_STATE("Could not get state, {}"),
  INVALID_STATE_TRANSITION("Cannot change state from {} to {}"),
  CANNOT_SET_OFFSET_RUNNING_STATE("Cannot set the source offset during a run."),
  CANNOT_CAPTURE_SNAPSHOT_WHEN_PIPELINE_NOT_RUNNING("Cannot capture snapshot when pipeline is not running."),
  INVALID_BATCH_SIZE("Invalid batch size supplied {}.");

  private final String msgTemplate;

  PipelineStateErrors(String msgTemplate) {
    this.msgTemplate = msgTemplate;
  }

  @Override
  public String getMessageTemplate() {
    return msgTemplate;
  }

}
