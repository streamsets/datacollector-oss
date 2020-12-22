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
package com.streamsets.pipeline.stage.config.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection;
import org.apache.http.HttpHost;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection.PORT_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection.SERVER_URL_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection.SECURITY_CONFIG_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection.TYPE;
import static com.streamsets.pipeline.stage.connection.elasticsearch.ElasticsearchConnection.USE_SECURITY_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.ACCESS_KEY_ID_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.ENDPOINT_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.SECURITY_PASSWORD_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.SECURITY_USER_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG_NAME;
import static com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig.SSL_TRUSTSTORE_PATH_CONFIG_NAME;

public abstract class ElasticsearchConfig {
  private static final String CONNECTION_CONFIG_NAME = "connection";

  public static final String SERVER_URL_CONFIG_PATH = CONNECTION_CONFIG_NAME + "." + SERVER_URL_CONFIG_NAME;
  public static final String PORT_CONFIG_PATH = CONNECTION_CONFIG_NAME + "." + PORT_CONFIG_NAME;
  public static final String USE_SECURITY_CONFIG_PATH = CONNECTION_CONFIG_NAME + "." + USE_SECURITY_CONFIG_NAME;

  private static final String SECURITY_CONFIG_PATH = CONNECTION_CONFIG_NAME + "." + SECURITY_CONFIG_CONFIG_NAME;

  public static final String ENDPOINT_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + ENDPOINT_CONFIG_NAME;
  public static final String SECURITY_USER_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + SECURITY_USER_CONFIG_NAME;
  public static final String SECURITY_PASSWORD_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + SECURITY_PASSWORD_CONFIG_NAME;
  public static final String ACCESS_KEY_ID_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + ACCESS_KEY_ID_CONFIG_NAME;
  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + SSL_TRUSTSTORE_PASSWORD_CONFIG_NAME;
  public static final String SSL_TRUSTSTORE_PATH_CONFIG_PATH = SECURITY_CONFIG_PATH + "." + SSL_TRUSTSTORE_PATH_CONFIG_NAME;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    connectionType = TYPE,
    defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
    label = "Connection",
    group = "#0",
    displayPosition = -500
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public ElasticsearchConnection connection = new ElasticsearchConnection();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Additional HTTP Params",
      description = "Additional HTTP Params.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public Map<String, String> params = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Detect Additional Nodes in Cluster",
      defaultValue = "false",
      description = "Select to automatically discover additional Elasticsearch nodes in the cluster. " +
          "Do not use if the Data Collector is on a different network from the cluster.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean clientSniff = false;

  public String createHostsString() {
    return connection.getServerURL().stream()
        .map(HttpHost::create)
        .map(HttpHost::toHostString)
        .collect(Collectors.joining(","));
  }
}
