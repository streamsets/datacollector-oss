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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.OAuthConfigBean;
import com.streamsets.pipeline.lib.http.PasswordAuthConfigBean;
import com.streamsets.pipeline.lib.http.SslConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientConfigBean.class);

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
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
      group = "HTTP"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Headers",
      description = "Headers to include in the request",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 11,
      elDefs = VaultEL.class,
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
      label = "Request Body",
      description = "Data that should be included as the body of the request",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 13,
      elDefs = VaultEL.class,
      lines = 2,
      dependsOn = "httpMethod",
      triggeredByValue = { "POST", "PUT", "DELETE" },
      group = "HTTP"
  )
  public String requestBody;

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

  @ConfigDefBean(groups = "SSL")
  public SslConfigBean sslConfig = new SslConfigBean();

  /**
   * Validates the parameters for this config bean.
   * @param context Stage Context
   * @param groupName Group name this bean is used in
   * @param prefix Prefix to the parameter names (e.g. parent beans)
   * @param issues List of issues to augment
   */
  public void init(Source.Context context, String groupName, String prefix, List<Stage.ConfigIssue> issues) {

    List<String> elConfigs = ImmutableList.of("resourceUrl", "headers", "requestBody");
    for (String configName : elConfigs) {
      ELVars vars = context.createELVars();
      ELEval eval = context.createELEval(configName);
      try {
        eval.eval(vars, resourceUrl, String.class);
      } catch (ELEvalException e) {
        LOG.error(Errors.HTTP_06.getMessage(), e.toString(), e);
        issues.add(context.createConfigIssue(groupName, prefix + "resourceUrl", Errors.HTTP_06, e.toString()));
      }
    }
  }
}
