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
package com.streamsets.lib.security.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import java.util.Map;

public class AuthenticationResourceHandler {

  static final Map AUTHENTICATION_OK = ImmutableMap.of("message", "Authentication succeeded");
  static final Map AUTHENTICATION_FAILED = ImmutableMap.of("message", "Authentication failed");

  private final Authentication authentication;
  private final boolean secureLoadBalancer;

  public AuthenticationResourceHandler(Authentication authentication, boolean secureLoadBalancer) {
    this.authentication = authentication;
    this.secureLoadBalancer = secureLoadBalancer;
  }

  @VisibleForTesting
  long getTimeNow() {
    return System.currentTimeMillis();
  }

  NewCookie createLoginCookie(HttpServletRequest req, SSOPrincipal principal) {
    String token = principal.getTokenStr();
    // if expires is negative, it means the cookie must be transient
    int expires = (principal.getExpires() <= -1)
        ? NewCookie.DEFAULT_MAX_AGE
        : (int) ((principal.getExpires() - getTimeNow()) / 1000);
    NewCookie authCookie = new NewCookie(
        HttpUtils.getLoginCookieName(),
        token,
        "/",
        null,
        null,
        expires,
        (req.isSecure() || secureLoadBalancer)
    );
    return authCookie;
  }

  public Response login(HttpServletRequest req, LoginJson login) {
    Utils.checkNotNull(login, "login");
    Response response;
    SSOPrincipal principal = authentication.validateUserCredentials(
        login.getUserName(),
        login.getPassword(),
        SSOPrincipalUtils.getClientIpAddress(req)
    );
    if (principal == null) {
      response = Response.status(Response.Status.FORBIDDEN).entity(AUTHENTICATION_FAILED).build();
    } else {
      String token = principal.getTokenStr();
      response = Response
          .ok()
          .header(SSOConstants.X_USER_AUTH_TOKEN, token)
          .entity(AUTHENTICATION_OK)
          .cookie(createLoginCookie(req, principal))
          .build();
      authentication.registerSession(principal);
    }
    return response;
  }

}
