/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.OAuthConfigBean;
import com.streamsets.pipeline.lib.http.PasswordAuthConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.HashMap;
import java.util.Map;

public class HttpClientConfigBean {

  @ConfigDefBean(groups = "HTTP")
  public BasicConfig basic = new BasicConfig();

  @ConfigDefBean(groups = "HTTP")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      defaultValue = "https://stream.twitter.com/1.1/statuses/sample.json",
      description = "Specify the streaming HTTP resource URL",
      elDefs = RecordEL.class,
      displayPosition = 10,
      group = "HTTP"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Headers",
      description = "Headers to include in the request",
      displayPosition = 11,
      group = "HTTP"
  )
  public Map<String, String> headers = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "HTTP Method",
      defaultValue = "GET",
      description = "HTTP method to send",
      displayPosition = 12,
      group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.GET;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Request Data",
      description = "Data that should be included as a part of the request",
      displayPosition = 13,
      lines = 2,
      dependsOn = "httpMethod",
      triggeredByValue = { "POST", "PUT", "DELETE" },
      group = "HTTP"
  )
  public String requestData;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Timeout",
      defaultValue = "1000",
      description = "HTTP request timeout in milliseconds.",
      displayPosition = 20,
      group = "HTTP"
  )
  public long requestTimeoutMillis = 1000;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      defaultValue = "STREAMING",
      displayPosition = 25,
      group = "HTTP"
  )
  @ValueChooserModel(HttpClientModeChooserValues.class)
  public HttpClientMode httpMode = HttpClientMode.STREAMING;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Polling Interval (ms)",
      defaultValue = "5000",
      displayPosition = 26,
      group = "HTTP",
      dependsOn = "httpMode",
      triggeredByValue = "POLLING"
  )
  public long pollingInterval = 5000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      displayPosition = 30,
      group = "HTTP"
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType authType = AuthenticationType.NONE;

  @ConfigDefBean(groups = "CREDENTIALS")
  public OAuthConfigBean oauth = new OAuthConfigBean();

  @ConfigDefBean(groups = "CREDENTIALS")
  public PasswordAuthConfigBean basicAuth = new PasswordAuthConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      displayPosition = 40,
      group = "HTTP"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Http Stream Delimiter",
      defaultValue = "\\r\\n",
      description = "Http stream may be delimited by a user-defined string. Common values are \\r\\n and \\n",
      displayPosition = 50,
      group = "HTTP"
  )
  public String entityDelimiter = "\\r\\n";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      defaultValue = "false",
      displayPosition = 60,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();
}
