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
package com.streamsets.pipeline.stage.processor.dedup;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

/* LC: For the first two - do we need to tell them what the value is? Can't we just tell them what it needs to be?
* Can we say:
* Maximum record count must be greater than zero
* Time to compare must be a positive integer or zero to opt out of a time comparison */

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  DEDUP_00("Maximum record count must be greater than zero, it is '{}'"),
  DEDUP_01("Time window must be zero (disabled) or greater than zero, it is '{}'"),
  DEDUP_02("Specify at least one field for comparison"),
  DEDUP_03("The estimated required memory for '{}' records is '{}'. The current maximum heap is '{}'. The " +
           "required memory must not exceed the maximum heap."),
  DEDUP_04("Error processing record. Reason: {}"),
  ;


  private final String msg;
  Errors(String msg) {
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
