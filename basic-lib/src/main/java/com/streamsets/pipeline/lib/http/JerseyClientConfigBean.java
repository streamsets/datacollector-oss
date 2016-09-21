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
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.origin.http.AuthenticationTypeChooserValues;
import com.streamsets.pipeline.stage.origin.http.Errors;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.glassfish.jersey.client.RequestEntityProcessing.CHUNKED;

public class JerseyClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(JerseyClientConfigBean.class);

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Request Transfer Encoding",
      defaultValue = "CHUNKED",
      displayPosition = 100,
      group = "#0"
  )
  @ValueChooserModel(RequestEntityProcessingChooserValues.class)
  public RequestEntityProcessing transferEncoding = CHUNKED;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connect Timeout",
      defaultValue = "0",
      description = "HTTP connection timeout in milliseconds. 0 means no timeout.",
      displayPosition = 120,
      group = "HTTP"
  )
  public int connectTimeoutMillis = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Read Timeout",
      defaultValue = "0",
      description = "HTTP read timeout in milliseconds. 0 means no timeout.",
      displayPosition = 130,
      group = "HTTP"
  )
  public int readTimeoutMillis = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Parallel Requests",
      defaultValue = "1",
      description = "Maximum number of requests to make in parallel.",
      displayPosition = 140,
      group = "HTTP"
  )
  public int numThreads = 1;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      displayPosition = 150,
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
      displayPosition = 160,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "SSL")
  public SslConfigBean sslConfig = new SslConfigBean();

  public void init(Stage.Context context, String groupName, String prefix, List<Stage.ConfigIssue> issues) {
    if (useProxy && !StringUtils.isEmpty(proxy.uri)) {
      try {
        new URI(proxy.uri).toURL();
      } catch (URISyntaxException | MalformedURLException e) {
        LOG.error("Invalid URL: " + proxy.uri, e);
        issues.add(context.createConfigIssue(groupName, prefix + "proxy.uri", Errors.HTTP_13, e.toString()));
      }
    }
  }
}
