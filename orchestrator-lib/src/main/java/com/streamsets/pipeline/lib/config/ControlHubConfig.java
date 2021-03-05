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
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
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
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "Control Hub User Name",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Control Hub Password",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  public CredentialValue password = () -> "";

  @ConfigDefBean
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
      clientBuilder.register(new UserAuthInjectionFilter(() -> getUserAuthToken()));
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
