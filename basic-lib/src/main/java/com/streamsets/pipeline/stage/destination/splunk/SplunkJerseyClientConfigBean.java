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
package com.streamsets.pipeline.stage.destination.splunk;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.AuthenticationTypeChooserValues;
import com.streamsets.pipeline.lib.http.HttpCompressionChooserValues;
import com.streamsets.pipeline.lib.http.HttpCompressionType;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.RequestEntityProcessingChooserValues;
import com.streamsets.pipeline.lib.http.logging.RequestLoggingConfigBean;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.glassfish.jersey.client.RequestEntityProcessing.BUFFERED;

// Copy of JerseyClientConfigBean, minus credentials, OAuth2
public class SplunkJerseyClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(SplunkJerseyClientConfigBean.class);

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Request Transfer Encoding",
      defaultValue = "BUFFERED",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  @ValueChooserModel(RequestEntityProcessingChooserValues.class)
  public RequestEntityProcessing transferEncoding = BUFFERED;


  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "HTTP Compression",
      defaultValue = "NONE",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  @ValueChooserModel(HttpCompressionChooserValues.class)
  public HttpCompressionType httpCompression = HttpCompressionType.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Connect Timeout",
      defaultValue = "0",
      description = "HTTP connection timeout in milliseconds. 0 means no timeout.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
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
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public int readTimeoutMillis = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType authType = AuthenticationType.NONE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      defaultValue = "false",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

  @ConfigDefBean(groups = "LOGGING")
  public RequestLoggingConfigBean requestLoggingConfig = new RequestLoggingConfigBean();
}
