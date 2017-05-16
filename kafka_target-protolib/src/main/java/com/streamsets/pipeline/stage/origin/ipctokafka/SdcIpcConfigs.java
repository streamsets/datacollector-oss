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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

public class SdcIpcConfigs extends HttpConfigs {

  public SdcIpcConfigs() {
    super(Groups.RPC.name(), "config.");
  }

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfigBean = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "20000",
      label = "RPC Listening Port",
      description = "Port number to listen for data. Must match one of the port numbers used by the SDC RPC " +
          "destination of the origin pipeline.",
      displayPosition = 10,
      group = "RPC",
      min = 1,
      max = 65535
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Max Concurrent Requests",
      description = "Maximum number of concurrent requests allowed by the origin. Configure based on the number " +
          "of incoming pipelines, volume of data, and Data Collector resources.",
      displayPosition = 15,
      group = "RPC",
      min = 1,
      max = 200
  )
  public int maxConcurrentRequests;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "RPC ID",
      description = "User-defined ID. Must match the RPC ID used by the RPC destination of the origin pipeline.",
      displayPosition = 20,
      elDefs = VaultEL.class,
      group = "RPC"
  )
  public String appId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "100000",
      label = "Max Batch Request Size (KB)",
      description = "Maximum batch request size in KB. This is the maximum size of data that can be transferred " +
          "in one RPC call.",
      displayPosition = 30,
      group = "RPC",
      min = 1,
      max = 500000
  )
  public int maxRpcRequestSize;

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

  @Override
  public int getMaxHttpRequestSizeKB() {
    return maxRpcRequestSize;
  }

  @Override
  public boolean isTlsEnabled() {
    return tlsConfigBean.isEnabled();
  }

  @Override
  public boolean isAppIdViaQueryParamAllowed() {
    return false;
  }

  @Override
  public TlsConfigBean getTlsConfigBean() {
    return tlsConfigBean;
  }
}
