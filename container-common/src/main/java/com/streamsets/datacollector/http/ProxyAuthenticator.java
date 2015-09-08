/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.main.RuntimeInfo;

import org.eclipse.jetty.security.*;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.security.Principal;
import java.util.Map;


public class ProxyAuthenticator extends LoginAuthenticator {
  private final RuntimeInfo runtimeInfo;
  private final LoginAuthenticator authenticator;
  public final static String AUTH_TOKEN = "auth_token";
  public final static String AUTH_USER = "auth_user";
  private final static String TOKEN_AUTHENTICATION_USER_NAME = "admin";

  public ProxyAuthenticator(LoginAuthenticator authenticator, RuntimeInfo runtimeInfo) {
    this.authenticator = authenticator;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void prepareRequest(ServletRequest request) {
    super.prepareRequest(request);
    authenticator.prepareRequest(request);
  }

  @Override
  public UserIdentity login(String username, Object password, ServletRequest request) {
    return authenticator.login(username, password, request);
  }

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
    super.setConfiguration(configuration);
    authenticator.setConfiguration(configuration);
  }

  @Override
  public LoginService getLoginService() {
    return authenticator.getLoginService();
  }

  @Override
  protected HttpSession renewSession(HttpServletRequest request, HttpServletResponse response) {
    return super.renewSession(request, response);
  }


  @Override
  public String getAuthMethod() {
    return authenticator.getAuthMethod();
  }

  @Override
  public boolean secureResponse(ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser) throws ServerAuthException {
    return authenticator.secureResponse(request, response, mandatory, validatedUser);
  }

  @Override
  public Authentication validateRequest(ServletRequest req, ServletResponse res, boolean mandatory)
    throws ServerAuthException {

    HttpServletRequest request = (HttpServletRequest)req;
    HttpSession session = request.getSession(true);
    Authentication authentication = (Authentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);

    if (authentication == null) {
      String authToken = request.getHeader(AUTH_TOKEN);
      String authUser = request.getHeader(AUTH_USER);

      if(authToken == null) {
        authToken = request.getParameter(AUTH_TOKEN);
        authUser = request.getParameter(AUTH_USER);

        if(authUser == null) {
          authUser = TOKEN_AUTHENTICATION_USER_NAME;
        }
      }

      if(authToken != null && runtimeInfo.isValidAuthenticationToken(authToken)) {
        HashLoginService loginService = (HashLoginService)getLoginService();
        Map<String, UserIdentity> usersMap = loginService.getUsers();
        UserIdentity userIdentity = usersMap.get(authUser);

        Authentication cached=new SessionAuthentication(getAuthMethod(), userIdentity, null);
        session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, cached);
      }


      if(this.authenticator instanceof FormAuthenticator) {
        //Handle redirecting to home page instead of REST API page & setting reverse proxy base path
        String pathInfo = request.getPathInfo();
        if("/j_security_check".equals(pathInfo)) {
          String basePath = request.getParameter("basePath");

          if(basePath == null || basePath.trim().length() == 0) {
            basePath = "/";
          }

          String redirectURL = (String)session.getAttribute(FormAuthenticator.__J_URI);
          if((redirectURL != null && (redirectURL.contains("rest/v1/") || redirectURL.contains("jmx"))) || !basePath.equals("/")) {
            session.setAttribute(FormAuthenticator.__J_URI, basePath);
          }
        }
      }

    }

    return authenticator.validateRequest(req, res, mandatory);
  }
}
