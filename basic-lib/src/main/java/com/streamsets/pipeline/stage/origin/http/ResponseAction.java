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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.Label;

public enum ResponseAction implements Label {
  RETRY_LINEAR_BACKOFF("Retry with linear backoff"),
  RETRY_EXPONENTIAL_BACKOFF("Retry with exponential backoff"),
  RETRY_IMMEDIATELY("Retry immediately"),
  STAGE_ERROR("Cause the stage to fail"),
  ERROR_RECORD("Generate an Error Record"),
  ;

  private final String label;

  ResponseAction(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
