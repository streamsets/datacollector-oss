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
package com.streamsets.datacollector.lib.emr;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum EMRErrors implements ErrorCode {
  EMR_0100("Invalid EMR configuration: {}"),
  EMR_0500("Invalid region specified: {}"),
  EMR_0510("Invalid access key and secret"),
  EMR_1100("Invalid {}: {}"),
  EMR_1110("{} bucket does not exist: {}"),
  EMR_1200("Cluster ID not found: {}"),
  EMR_1250("Invalid Cluster ID: {}"),
  EMR_2000("EC2 Subnet ID not found: {}"),
  EMR_3000("{} does not exist: {}"),
  ;
  private final String msg;

  EMRErrors(String msg) {
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
