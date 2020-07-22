/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.startPipeline;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum StartPipelineErrors implements ErrorCode {
  START_PIPELINE_01("Failed to connect to execution engine: {}"),
  START_PIPELINE_02("Pipeline ID {} does not exist in execution engine: {}"),
  START_PIPELINE_03("Configuration value is required for pipeline ID, at index: {}"),
  START_PIPELINE_04("Failed to start pipeline: {}"),
  START_PIPELINE_05("Failed to fetch a unique pipeline from the given pipeline name: {}, found {} entries"),
  START_PIPELINE_06("Failed to fetch pipeline status: {}"),
  ;

  private final String msg;
  StartPipelineErrors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }

}
