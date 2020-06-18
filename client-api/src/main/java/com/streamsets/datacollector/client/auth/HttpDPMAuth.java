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
package com.streamsets.datacollector.client.auth;

import org.glassfish.jersey.client.filter.CsrfProtectionFilter;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;


public class HttpDPMAuth extends AbstractAuthentication {
  private static final String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";
  private static final String AUTHENTICATION_COOKIE_PREFIX = "SS-SSO-";
  private String dpmBaseURL;

  @Override
  public void setDPMBaseURL(String dpmBaseURL) {
    if (dpmBaseURL != null && dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }
    this.dpmBaseURL = dpmBaseURL;
  }

  @Override
  public void setHeader(Invocation.Builder builder, String tokenStr) {
    builder.header(X_USER_AUTH_TOKEN, tokenStr);
  }

  @Override
  public String login() {
    Response response = null;
    try {
      Map<String, String> loginJson = new HashMap<>();
      loginJson.put("userName", this.username);
      loginJson.put("password", this.password);
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/public-rest/v1/authentication/login")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .post(Entity.json(loginJson));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException("DPM Login failed, status code '" +
            response.getStatus() + "': " +
            response.readEntity(String.class)
        );
      }

      return response.getHeaderString(X_USER_AUTH_TOKEN);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  @Override
  public void logout(String userAuthToken) {
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/_logout")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(X_USER_AUTH_TOKEN, userAuthToken)
          .cookie(AUTHENTICATION_COOKIE_PREFIX + "LOGIN", userAuthToken)
          .get();
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

}
