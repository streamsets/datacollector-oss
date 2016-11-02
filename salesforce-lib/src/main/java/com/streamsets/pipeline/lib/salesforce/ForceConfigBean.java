/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;

public class ForceConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Username",
      description = "Salesforce username, in the form user@example.com",
      displayPosition = 10,
      elDefs = VaultEL.class,
      group = "FORCE"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Password",
      description = "Salesforce password, or an EL to load the password from a resource, for example, ${runtime:loadResource('forcePassword.txt',true)}",
      displayPosition = 20,
      elDefs = VaultEL.class,
      group = "FORCE"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "login.salesforce.com",
      label = "Auth Endpoint",
      description = "Salesforce SOAP API Authentication Endpoint: login.salesforce.com for production/Developer Edition, test.salesforce.com for sandboxes",
      displayPosition = 30,
      group = "FORCE"
  )
  public String authEndpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "38.0",
      label = "API Version",
      description = "Salesforce API Version",
      displayPosition = 40,
      group = "FORCE"
  )
  public String apiVersion;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Compression",
      displayPosition = 1000
  )
  public boolean useCompression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Show Debug Trace",
      displayPosition = 1010
  )
  public boolean showTrace;
}
