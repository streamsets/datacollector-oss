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
package com.streamsets.datacollector.creation;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.event.client.impl.MovedDpmJerseyClientFilter;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConnectionConfigurationJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Map;

public class ConnectionRetriever {

  private static final String CONNECTION_RETRIEVAL_PATH_PREFIX = "connection/rest/v1/connection/";
  private static final String CONNECTION_RETRIEVAL_PATH_POSTFIX = "/configs";
  private static final String USER_PARAM = "user";

  private final RuntimeInfo runtimeInfo;

  public ConnectionRetriever(Configuration configuration, RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  public ConnectionConfiguration get(String connectionId, ConfigInjector.Context context) {
    if (runtimeInfo.isDPMEnabled()) {
      ClassLoader current = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      try {
        WebTarget target = getClient().target(getDpmClientInfo().getDpmBaseUrl() + CONNECTION_RETRIEVAL_PATH_PREFIX + connectionId + CONNECTION_RETRIEVAL_PATH_POSTFIX);
        target = target.queryParam(USER_PARAM, context.getUser());

        Invocation.Builder builder = target.request();
        builder.header(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
        for (Map.Entry<String, String> entry : getDpmClientInfo().getHeaders().entrySet()) {
          builder.header(entry.getKey(), entry.getValue().trim());
        }

        Response response = builder.get();
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
          ConnectionConfigurationJson ccj = response.readEntity(ConnectionConfigurationJson.class);
          return BeanHelper.unwrapConnectionConfiguration(ccj);
        } else {
          String error = response.readEntity(String.class);
          context.createIssue(CreationError.CREATION_1104, response.getStatusInfo().getStatusCode(), error);
        }
      } catch (Exception e) {
        context.createIssue(CreationError.CREATION_1104, e.getMessage(), e);
      } finally {
        Thread.currentThread().setContextClassLoader(current);
      }
    } else {
      context.createIssue(CreationError.CREATION_1105);
    }
    return null;
  }

  private DpmClientInfo getDpmClientInfo() {
    return runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY);
  }

  @VisibleForTesting
  ClientConfig getClientConfig() {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.register(new MovedDpmJerseyClientFilter(getDpmClientInfo()));
    return clientConfig;
  }

  @VisibleForTesting
  Client getClient() {
    return ClientBuilder.newClient(getClientConfig());
  }

}
