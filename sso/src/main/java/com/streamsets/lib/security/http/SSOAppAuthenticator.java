/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.lib.security.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SSOAppAuthenticator implements Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOAppAuthenticator.class);

  static final String FORBIDDEN_JSON_STR;

  static {
    try {
      FORBIDDEN_JSON_STR = new ObjectMapper().writeValueAsString(ImmutableMap.of(
          "ISSUES",
          ImmutableList.of(ImmutableMap.of("code", "SSO_01`", "message", "Forbidden, user not authenticated"))
      ));
    } catch (Exception ex) {
      throw new RuntimeException("Shouldn't happen: " + ex.toString(), ex);
    }
  }

  private final SSOService ssoService;

  private final Cache<String, SSOAuthenticationUser> knownTokens;

  public SSOAppAuthenticator(String appContext, SSOService ssoService) {
    LOG.debug("App context '{}' using SSO for applications '{}'", appContext, ssoService);
    this.ssoService = ssoService;

    // register a listener with the SSOService to listen to invalidations from the security service
    ssoService.setListener(new SSOService.Listener() {
      @Override
      public void invalidate(List<String> tokenIds) {
        SSOAppAuthenticator.this.invalidate(tokenIds);
      }
    });

    knownTokens = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).build();
  }

  void invalidate(List<String> tokenIds) {
    for (String id : tokenIds) {
      invalidateToken(id);
    }
  }

  void cacheToken(String authToken, SSOAuthenticationUser user) {
    knownTokens.put(authToken, user);
  }

  SSOAuthenticationUser getUserFromCache(String authToken) {
    return knownTokens.getIfPresent(authToken);
  }

  void invalidateToken(String id) {
    if (knownTokens.getIfPresent(id) != null) {
      knownTokens.invalidate(id);
      LOG.debug("Token '{}' invalidated", id);
    } else {
      LOG.debug("Token '{}' not found, it may have expired already", id);
    }
  }

  @Override
  public void setConfiguration(AuthConfiguration configuration) {
  }

  @Override
  public String getAuthMethod() {
    return SSOConstants.AUTHENTICATION_METHOD;
  }

  @Override
  public void prepareRequest(ServletRequest request) {
  }

  String getRequestInfoForLogging(HttpServletRequest request) {
    StringBuffer requestUrl = request.getRequestURL();
    String qs = request.getQueryString();
    if (qs != null) {
      requestUrl.append("?").append(qs);
    }
    String remoteAddress = request.getRemoteAddr();
    requestUrl.append(" , from : ").append(remoteAddress);
    return requestUrl.toString();
  }

  /*
   * Terminates the request with an HTTP forbidden response
   */
  Authentication returnForbidden(HttpServletRequest httpReq, HttpServletResponse httpRes, String logMessageTemplate)
      throws ServerAuthException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(logMessageTemplate, getRequestInfoForLogging(httpReq));
    }
    try {
      httpRes.sendError(HttpServletResponse.SC_FORBIDDEN);
      httpRes.setContentType("application/json");
      httpRes.getWriter().println(FORBIDDEN_JSON_STR);
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could send a FORBIDDEN (403) response: {}", ex.toString(), ex));
    }
    return Authentication.SEND_FAILURE;
  }

  Authentication revalidateToken(
      SSOAuthenticationUser authenticationUser,
      HttpServletRequest httpReq,
      HttpServletResponse httpRes
  ) throws ServerAuthException {
    Authentication ret;
    if (System.currentTimeMillis() - authenticationUser.getValidationTime() >
        ssoService.getValidateAppTokenFrequency()) {
      LOG.debug("Revalidating token for '{}'", authenticationUser.getSSOUserPrincipal().getPrincipalId());
      String authToken = authenticationUser.getSSOUserPrincipal().getTokenStr();
      String componentId = authenticationUser.getSSOUserPrincipal().getPrincipalId();
      SSOUserPrincipal principal = ssoService.validateAppToken(authToken, componentId);
      if (principal != null) {
        authenticationUser = new SSOAuthenticationUser(principal);
        cacheToken(authToken, authenticationUser);
        ret = authenticationUser;
      } else {
        invalidateToken(authToken);
        ret = returnForbidden(httpReq, httpRes, "Request '{}', token could not be revalidated");
      }
    } else {
      LOG.debug("No need to revalidate token for '{}' yet", authenticationUser.getSSOUserPrincipal().getPrincipalId());
      ret = authenticationUser;
    }
    return ret;
  }

  Authentication validateToken(
      String authToken, String componentId, HttpServletRequest httpReq, HttpServletResponse httpRes
  ) throws ServerAuthException {
    Authentication ret;
    SSOUserPrincipal principal = ssoService.validateAppToken(authToken, componentId);
    if (principal != null) {
      if (!principal.getPrincipalId().equals(componentId)) {
        ret = returnForbidden(httpReq, httpRes, "App token does not match System ID: {}");
      } else {
        LOG.debug("App Token '{}' verified", principal);
        SSOAuthenticationUser user = new SSOAuthenticationUser(principal);
        cacheToken(authToken, user);
        ret = user;
      }
    } else {
      ret = returnForbidden(httpReq, httpRes, "Request '{}', token not valid");
    }
    return ret;
  }

  String getAppAuthToken(HttpServletRequest req) {
    return req.getHeader(SSOConstants.X_APP_AUTH_TOKEN);
  }

  String getAppComponentId(HttpServletRequest req) {
    return req.getHeader(SSOConstants.X_APP_COMPONENT_ID);
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    Authentication ret;
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    if (((HttpServletRequest) request).getHeader(SSOConstants.X_REST_CALL) == null) {
      ret = returnForbidden(httpReq, httpRes, "Request is not a REST call: {}");
    } else {
      String authToken = getAppAuthToken(httpReq);
      String componentId = getAppComponentId(httpReq);
      if (authToken == null) {
        ret = returnForbidden(httpReq, httpRes, "Request does not have an app authentication token: {}");
      } else if (componentId == null) {
        ret = returnForbidden(httpReq, httpRes, "Request does not have an app system ID: {}");
      } else {
        ssoService.refresh();
        SSOAuthenticationUser user = getUserFromCache(authToken);
        if (user != null && !user.getSSOUserPrincipal().getPrincipalId().equals(componentId)) {
          ret = returnForbidden(httpReq, httpRes, "App token does not match System ID: {}");
        } else {
          ret = user;
          if (ret != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Request '{}' token found in cache", getRequestInfoForLogging(httpReq));
            }
            ret = revalidateToken(user, httpReq, httpRes);
          } else {
            if (mandatory) {
              LOG.debug("Request '{}' validate token", getRequestInfoForLogging(httpReq));
              ret = validateToken(authToken, componentId, httpReq, httpRes);
            } else {
              // doing nothing, the request does not need authentication

              if (LOG.isDebugEnabled()) {
                LOG.debug("Request '{}' does not require authentication", getRequestInfoForLogging(httpReq));
              }
              ret = Authentication.NOT_CHECKED;
            }
          }
        }
      }
    }
    return ret;
  }

  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return true;
  }

}
