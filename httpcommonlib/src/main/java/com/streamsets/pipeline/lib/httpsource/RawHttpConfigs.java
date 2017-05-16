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
package com.streamsets.pipeline.lib.httpsource;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

public class RawHttpConfigs extends HttpConfigs {

  public RawHttpConfigs() {
    super("HTTP", "config.");
  }

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "8000",
      label = "HTTP Listening Port",
      description = "HTTP endpoint to listen for data.",
      displayPosition = 10,
      group = "HTTP",
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
      group = "HTTP",
      min = 1,
      max = 200
  )
  public int maxConcurrentRequests;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Application ID",
      description = "Only HTTP requests presenting this token will be accepted.",
      displayPosition = 20,
      elDefs = VaultEL.class,
      group = "HTTP"
  )
  public String appId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Application ID in URL",
      description = "Use when the application ID is included in a query parameter in the URL instead of in the request header - http://localhost:8000?sdcApplicationId=<Application ID>",
      displayPosition = 21,
      group = "HTTP"
  )
  public boolean appIdViaQueryParamAllowed;

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  @Override
  public String getAppId() {
    return appId;
  }

  private int maxHttpRequestSizeKB = -1;

  // in MBs
  public void setMaxHttpRequestSizeKB(int size) {
    maxHttpRequestSizeKB = size;
  }

  @Override
  public int getMaxHttpRequestSizeKB() {
    return maxHttpRequestSizeKB;
  }

  @Override
  public boolean isTlsEnabled() {
    return tlsConfigBean.isEnabled();
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return appIdViaQueryParamAllowed;
  }

  @Override
  public TlsConfigBean getTlsConfigBean() {
    return tlsConfigBean;
  }

}
