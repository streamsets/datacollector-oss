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
package com.streamsets.pipeline.stage.origin.sqs;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  SQS_01("Endpoint must be specified if region is OTHER"),
  SQS_02("No queues with name prefix {} found"),
  SQS_03("Internal error checking worker status: {}"),
  SQS_04("Recoverable data parser error when parsing record; continuing: {}"),
  SQS_05("Error attempting to build data parser: {}"),
  SQS_06("Internal error running SQS source: {}"),
  SQS_07("Error attempting to parse record in SQS source: {}"),
  SQS_08("Error attempting to delete (commit) messages with IDs {} in queue {}: {}"),
  SQS_09("No queue URLs found for any configured prefixes"),
  SQS_10("Error initilizing AWS client proxy: {}"),
  SQS_11("Error initilizing AWS client credentials: {}"),
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
