/*
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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.http;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.origin.http.AuthenticationTypeChooserValues;
import org.glassfish.jersey.client.RequestEntityProcessing;

import static org.glassfish.jersey.client.RequestEntityProcessing.CHUNKED;

public class JerseyClientConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Transfer Encoding",
      defaultValue = "CHUNKED",
      displayPosition = 100,
      group = "#0"
  )
  @ValueChooserModel(RequestEntityProcessingChooserValues.class)
  public RequestEntityProcessing transferEncoding = CHUNKED;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Timeout",
      defaultValue = "1000",
      description = "HTTP request timeout in milliseconds.",
      displayPosition = 110,
      group = "HTTP"
  )
  public long requestTimeoutMillis = 1000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum parallel requests",
      defaultValue = "10",
      description = "Maximum number of requests to make in parallel.",
      displayPosition = 120,
      group = "HTTP"
  )
  public int numThreads = 10;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      displayPosition = 130,
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
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      defaultValue = "false",
      displayPosition = 140,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "SSL")
  public SslConfigBean sslConfig = new SslConfigBean();
}
