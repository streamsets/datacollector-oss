/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.connection.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Elasticsearch",
    type = ElasticsearchConnection.TYPE,
    description = "Elasticsearch connector",
    version = 1,
    upgraderDef = "upgrader/ElasticsearchConnection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR}
)
@ConfigGroups(ElasticsearchConnectionGroups.class)
public class ElasticsearchConnection {

  public static final String TYPE = "STREAMSETS_ELASTICSEARCH";

  public static final String SERVER_URL_CONFIG_NAME = "serverUrl";
  public static final String PORT_CONFIG_NAME = "port";
  public static final String SECURITY_CONFIG_CONFIG_NAME = "securityConfig";
  public static final String USE_SECURITY_CONFIG_NAME = "useSecurity";

  @ConfigDefBean(groups = "#1")
  public SecurityConfig securityConfig = new SecurityConfig();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HTTP URLs",
      defaultValue = "http://localhost",
      description = "Elasticserarch HTTP/HTTPS Endpoint.",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String serverUrl;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HTTP Port",
      defaultValue = "9200",
      description = "Port number for Elasticsearch server. Default is 9200 for http and 443 for https",
      displayPosition = 11,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String port = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Security",
      defaultValue = "true",
      description = "Use Security",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean useSecurity = true;

  public List<String> getServerURL() {
    int parsedPort = parsePort();
    return Arrays.stream(serverUrl.split(",", -1)).map(url -> {
      String result = url.matches(".+:\\d+$") || parsedPort == 0 ? url : url + ":" + port;
      result = (!result.matches("^https?:.+") && useSecurity && securityConfig.enableSSL ? "https://" : "") + result;
      return result;
    }).collect(Collectors.toList());
  }

  private int parsePort() {
    int parsedPort = 0;
    if (!port.trim().isEmpty()) {
      try {
        parsedPort = Integer.parseInt(port);
      } catch (final NumberFormatException ex) {
        // Will use the default value;
      }
    }
    return parsedPort;
  }

  // Display position in SecurityConfig starts where this stops. This is because this config is also available
  // on error stage where there is only one tab an hence all configs are sequential.
}
