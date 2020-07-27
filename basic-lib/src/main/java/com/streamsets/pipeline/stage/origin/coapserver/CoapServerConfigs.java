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
package com.streamsets.pipeline.stage.origin.coapserver;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.Collections;
import java.util.Map;

public class CoapServerConfigs {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5683",
      label = "CoAP Listening Port",
      description = "CoAP endpoint to listen for data.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COAP",
      min = 1,
      max = 65535
  )
  public int port;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Concurrent Requests",
      description = "Maximum number of concurrent requests allowed by the origin.",
      displayPosition = 15,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COAP",
      min = 1,
      max = 200
  )
  public int maxConcurrentRequests;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "sdc",
      label = "Resource Name",
      description = "CoAP Resource Name",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "COAP"
  )
  public CredentialValue resourceName;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Network Configuration",
      description = "Additional network configuration properties. Values here override default values.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "COAP"
  )
  public Map<String, Object> networkConfigs = Collections.emptyMap();

}
