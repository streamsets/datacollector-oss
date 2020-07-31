/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.controlHub;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.logging.RequestLoggingConfigBean;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class HttpClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(com.streamsets.pipeline.lib.http.JerseyClientConfigBean.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connection Timeout (ms)",
      defaultValue = "0",
      description = "HTTP connection timeout in milliseconds. Use 0 for no timeout.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public int connectTimeoutMillis = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Read Timeout (ms)",
      defaultValue = "0",
      description = "HTTP read timeout in milliseconds. Use 0 for no timeout.",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public int readTimeoutMillis = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Parallel Requests",
      defaultValue = "1",
      description = "Maximum number of requests to make in parallel",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public int numThreads = 1;

  @ConfigDefBean(groups = "CREDENTIALS")
  public ControlHubAuthConfigBean basicAuth = new ControlHubAuthConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      description = "Use an HTTP proxy to connect to the Control Hub API",
      defaultValue = "false",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

  @ConfigDefBean(groups = "LOGGING")
  public RequestLoggingConfigBean requestLoggingConfig = new RequestLoggingConfigBean();

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
