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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  OPC_UA_01("No desired OPC UA endpoints returned"),
  OPC_UA_02("Failed to connect : {}"),
  OPC_UA_03("Field cannot be empty"),
  OPC_UA_04("Failed to initialize Node IDs : {}"),
  OPC_UA_05("Failed to create Monitoring item for nodeId={} (status={})"),
  OPC_UA_06("Enable TLS for security policy: {}"),
  OPC_UA_07("No variable Node IDs found from provided root Node ID"),
  OPC_UA_08("Failed to refresh Node IDs: {})"),
  OPC_UA_09("Failed to process data : {}"),
  OPC_UA_10("Failed during browsing nodeId={} failed: {}"),
  OPC_UA_11("Failed to initialize Node IDs, values cannot be empty or null: {}"),
  OPC_UA_12("Failed to read data : {}"),
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
