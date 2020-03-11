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
package com.streamsets.pipeline.stage.pubsub.lib;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  PUBSUB_02("Pipeline force stopped while waiting for pending messages."),
  PUBSUB_03("Supplied credentials do not have permission to access the specified subscription."),
  PUBSUB_04("Error validating permissions: '{}'"),
  PUBSUB_05("Failed to parse message: '{}'"),

  PUBSUB_06("Error serializing record: '{}'"),
  PUBSUB_07("Failed to create publisher for topic '{}' due to: '{}'"),
  PUBSUB_08("Error publishing message: '{}'"),
  PUBSUB_09("Invalid Limit Exceeded Behaviour value: '{}'"),
  PUBSUB_10("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
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
