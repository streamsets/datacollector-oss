/*
 * Copyright 2021  StreamSets Inc.
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
package com.streamsets.pipeline.lib.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.http.GrizzlyClientCustomizer;
import com.streamsets.pipeline.lib.startJob.StartJobErrors;
import com.streamsets.pipeline.stage.processor.controlHub.HttpClientConfigBean;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ControlHubConfig {
  @VisibleForTesting
  static final String ALLOW_API_USER_CREDENTIALS = "com.streamsets.pipeline.stage.orchestration.schApiUserCredentials.enabled";

  @VisibleForTesting
  static final String X_APP_AUTH_TOKEN = "X-SS-App-Auth-Token";
  @VisibleForTesting
  static final String X_APP_COMPONENT_ID = "X-SS-App-Component-Id";


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com",
      label = "Control Hub URL",
      description = "URL for the Control Hub API",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String baseUrl = "https://cloud.streamsets.com";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "API_USER_CREDENTIALS",
      displayPosition = 20,
      group = "#1"
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType authenticationType = AuthenticationType.API_USER_CREDENTIALS;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "Control Hub User Name",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "authenticationType",  triggeredByValues = "USER_PASSWORD")
      }
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Control Hub Password",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "authenticationType",  triggeredByValues = "USER_PASSWORD")
      }
  )
  public CredentialValue password = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Auth ID",
      description = "Control Hub User Auth ID",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "authenticationType",  triggeredByValues = "API_USER_CREDENTIALS")
      }
  )
  public CredentialValue componentId = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Control Hub User Auth Token",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#1",
      dependencies = {
          @Dependency(configName = "authenticationType",  triggeredByValues = "API_USER_CREDENTIALS")
      }
  )
  public CredentialValue authToken = () -> "";

  @ConfigDefBean(groups = { "#2", "#3", "#4", "#5"})
  public HttpClientConfigBean client = new HttpClientConfigBean();

  private ClientBuilder userAuthClientBuilder;
  private ClientBuilder clientBuilder;

  private ClientBuilder createClientBuilder(boolean hasTlsConfigs, boolean hastHttpProxyConfigs) {
    ClientConfig clientConfig = new ClientConfig()
        .property(ClientProperties.CONNECT_TIMEOUT, client.connectTimeoutMillis)
        .property(ClientProperties.READ_TIMEOUT, client.readTimeoutMillis)
        .property(ClientProperties.ASYNC_THREADPOOL_SIZE, client.numThreads);
    if(hastHttpProxyConfigs) {
      clientConfig = clientConfig.connectorProvider(new GrizzlyConnectorProvider(new GrizzlyClientCustomizer(
          true,
          client.proxy.username.get().isEmpty() ? null : client.proxy.username.get(),
          client.proxy.password.get().isEmpty() ? null : client.proxy.password.get()
      )));
    }

    ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);
    if (hasTlsConfigs) {
      clientBuilder.sslContext(client.tlsConfig.getSslContext());
    }
    clientBuilder.register(new SchCsrfProtectionFilter("orchestration"));
    return clientBuilder;
  }

  public boolean init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    client.init(context, "HTTP", "conf.controlHubConfig.client.", issues);
    boolean ok = client.tlsConfig.init(context, "CONTROL_HUB", "conf.controlHubConfig.client.tlsConfig", issues);
    if (ok) {
      userAuthClientBuilder = createClientBuilder(client.tlsConfig.tlsEnabled, client.useProxy);
      clientBuilder = createClientBuilder(client.tlsConfig.tlsEnabled, client.useProxy);
      switch (authenticationType) {
        case USER_PASSWORD:
          clientBuilder.register(new UserAuthInjectionFilter(() -> getUserAuthToken()));
          break;
        case API_USER_CREDENTIALS:
          if (!context.getConfiguration().get(ALLOW_API_USER_CREDENTIALS, false)) {
            Stage.ConfigIssue issue = context.createConfigIssue(
                com.streamsets.pipeline.lib.startJob.Groups.JOB.name(),
                "conf.controlHubConfig.authenticationType",
                StartJobErrors.START_JOB_09
            );
            issues.add(issue);
          } else {
            clientBuilder.register(new MovedDpmJerseyClientFilter(new DpmClientInfo() {
              @Override
              public String getDpmBaseUrl() {
                return ControlHubConfig.this.baseUrl;
              }

              @Override
              public Map<String, String> getHeaders() {
                return ImmutableMap.of(
                    X_APP_COMPONENT_ID.toLowerCase(),
                    ControlHubConfig.this.componentId.get(),
                    X_APP_AUTH_TOKEN.toLowerCase(),
                    ControlHubConfig.this.authToken.get()
                );
              }

              @Override
              public void setDpmBaseUrl(String dpmBaseUrl) {
                //LOG
                ControlHubConfig.this.baseUrl = dpmBaseUrl;
              }
            }));
          }
          break;

      }
    }
    return ok;
  }

  public ClientBuilder getClientBuilder() {
    return clientBuilder;
  }

  @VisibleForTesting
  ClientBuilder getUserAuthClientBuilder() {
    return userAuthClientBuilder;
  }

  @VisibleForTesting
  String getUserAuthToken() {
    // 1. Login to DPM to get user auth token
    Response response = null;
    try {
      Map<String, String> loginJson = new HashMap<>();
      loginJson.put("userName", username.get());
      loginJson.put("password", password.get());
      response = getUserAuthClientBuilder().build()
          .target(baseUrl + "security/public-rest/v1/authentication/login")
          .request()
          .post(Entity.json(loginJson));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new StageException(
            StartJobErrors.START_JOB_01,
            response.getStatus(),
            response.readEntity(String.class)
        );
      }
      return response.getHeaderString(Constants.X_USER_AUTH_TOKEN);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

}
