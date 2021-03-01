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
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum PreviewError implements ErrorCode {

  PREVIEW_0001("No task is running"),
  PREVIEW_0002("Could not retrieve the preview output : {}"),
  PREVIEW_0003("Encountered error while previewing : {}"),

  // dynamic preview errors

  PREVIEW_0101("Error executing {} actions in dynamic preview: {}"),
  PREVIEW_0102("Error executing preview event for dynamic preview; no immediate result available from which to fetch" +
      " previewer ID"),
  PREVIEW_0103("Error executing dynamic preview: could not detect generated pipeline ID after running before actions"),
  PREVIEW_0104("Error executing dynamic preview: remote exception: {}"),
  PREVIEW_0105("Error executing dynamic preview: previewer is unavailable"),
  PREVIEW_0106("Error executing dynamic preview: could not find stagelib definition for stage {} in library {}"),
  ;

  private final String msg;

  PreviewError(String msg) {
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
