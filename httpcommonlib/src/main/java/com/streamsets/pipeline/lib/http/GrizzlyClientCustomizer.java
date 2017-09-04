/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.http;

import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Realm;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.grizzly.connector.GrizzlyConnectorProvider;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;

/**
 * The underlying Grizzly version used in this connector doesn't send the Proxy-Authorization header
 * when connecting to HTTPS urls (when proxy authentication is required). This is a workaround
 * to always target the proxy preemptively with an authorization header.
 * This workaround was discovered in: https://github.com/AsyncHttpClient/async-http-client/issues/996
 *
 * Note that BASIC auth is the only supported auth by this patch as there is no configuration exposed
 * to the user to select a different proxy authentication mode currently.
 */
public class GrizzlyClientCustomizer implements GrizzlyConnectorProvider.AsyncClientCustomizer {
  private final boolean useProxy;
  private final String username;
  private final String password;

  public GrizzlyClientCustomizer(boolean useProxy, String username, String password) {
    this.useProxy = useProxy;
    this.username = username;
    this.password = password;
  }

  @Override
  public AsyncHttpClientConfig.Builder customize(
      Client client, Configuration config, AsyncHttpClientConfig.Builder configBuilder
  ) {
    if (useProxy && !StringUtils.isEmpty(username)) {
      Realm realm = new Realm.RealmBuilder().setScheme(Realm.AuthScheme.BASIC)
          .setUsePreemptiveAuth(true)
          .setTargetProxy(true)
          .setPrincipal(username)
          .setPassword(password)
          .build();

      configBuilder.setRealm(realm);
    }
    return configBuilder;
  }
}
