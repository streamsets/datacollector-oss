/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.grpcclient;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.HashMap;
import java.util.Map;

public class GrpcClientConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      defaultValue = "localhost:50051",
      description = "Specify the gRPC URL",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GRPC"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Service Method",
      defaultValue = "helloworld.Greeter/SayHello",
      description = "Specify the service method for gRPC in serviceName/methodName format",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "GRPC"
  )
  public String serviceMethod = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Request Data",
      description = "Data to send as an argument for the gRPC service method",
      displayPosition = 25,
      displayMode = ConfigDef.DisplayMode.BASIC,
      lines = 2,
      group = "GRPC"
  )
  public String requestData = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Method Type",
      description = "Type of service method that the origin calls",
      defaultValue = "UNARY_RPC",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GRPC"
  )
  @ValueChooserModel(GrpcClientModeChooserValues.class)
  public GrpcClientMode gRPCMode = GrpcClientMode.UNARY_RPC;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Polling Interval (ms)",
      defaultValue = "5000",
      displayPosition = 35,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GRPC",
      dependsOn = "gRPCMode",
      triggeredByValue = "UNARY_RPC"
  )
  public long pollingInterval = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connect Timeout (secs)",
      description = "The maximum time, in seconds, to wait for connection to be established.",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "10",
      displayPosition = 40,
      group = "GRPC"
  )
  public long connectTimeout = 10;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Keep Alive Time (secs)",
      description = "The maximum idle time in seconds, after which a keepalive probe is sent. " +
          "If the connection remains idle and no keepalive response is received for this same period then the " +
          "connection is closed and the operation fails.",
      defaultValue = "10",
      displayPosition = 45,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GRPC"
  )
  public long keepaliveTime = 10;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional Headers",
      description = "These headers will also be included in reflection requests to a server.",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GRPC"
  )
  public Map<String, String> addlHeaders = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Emit Defaults",
      description = "Emit default values for responses",
      defaultValue = "false",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "GRPC"
  )
  public boolean emitDefaults = false;

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Insecure",
      description = "Skip server certificate and domain verification. (NOT SECURE!).",
      defaultValue = "false",
      displayPosition = 1001,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TLS",
      dependsOn = "tlsConfig.tlsEnabled",
      triggeredByValue = "true"
  )
  public boolean insecure = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Authority",
      description = "Value of :authority pseudo-header to be use with underlying HTTP/2 requests. " +
          "It defaults to the given address.",
      displayPosition = 1102,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TLS",
      dependsOn = "tlsConfig.tlsEnabled",
      triggeredByValue = "true"
  )
  public String authority = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Server Name",
      description = "Override server name when validating TLS certificate.",
      displayPosition = 1103,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "TLS",
      dependsOn = "tlsConfig.tlsEnabled",
      triggeredByValue = "true"
  )
  public String serverName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "gRPC payload data format",
      defaultValue = "JSON",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

}
