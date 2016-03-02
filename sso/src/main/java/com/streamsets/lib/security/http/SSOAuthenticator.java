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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

public class SSOAuthenticator implements Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOAuthenticator.class);

  private final static Set<String> TOKEN_PARAM_SET = ImmutableSet.of(SSOConstants.USER_AUTH_TOKEN_PARAM);

  public static final String CONFIG_PREFIX = "http.authentication.sso.";

  static final String FORBIDDEN_JSON_STR;

  static {
    try {
      FORBIDDEN_JSON_STR = new ObjectMapper().writeValueAsString(ImmutableMap.of(
          "ISSUES",
          ImmutableList.of(ImmutableMap.of("code", "SSO_00", "message", "Forbidden, user not authenticated"))
      ));
    } catch (Exception ex) {
      throw new RuntimeException("Shouldn't happen: " + ex.toString(), ex);
    }
  }

  private final SSOService ssoService;

  // crossreference to support invalidation requests from login service
  private final Map<SSOAuthenticationUser, HttpSession> knownTokens;

  public SSOAuthenticator(String appContext, SSOService ssoService) {
    LOG.debug("App context '{}' using SSO Service '{}'", appContext, ssoService);
    this.ssoService = ssoService;

    // register a listener with the SSOService to listen to invalidations from the security service
    ssoService.setListener(new SSOService.Listener() {
      @Override
      public void invalidate(List<String> tokenIds) {
        SSOAuthenticator.this.invalidate(tokenIds);
      }
    });

    // using a weak reference map so we don't worry about purging the map because of session invalidation
    knownTokens = Collections.synchronizedMap(new WeakHashMap<SSOAuthenticationUser, HttpSession>());
  }

  void invalidate(List<String> tokenIds) {
    for (String id : tokenIds) {
      invalidateToken(id);
    }
  }

  void invalidateToken(String id) {
    HttpSession session = knownTokens.remove(SSOAuthenticationUser.createForInvalidation(id));
    if (session != null) {
      LOG.debug("Token '{}' and its session invalidated", id);
      try {
        session.invalidate();
      } catch (IllegalStateException ex) {
        // ignoring
      }
    } else {
      LOG.debug("Token '{}' not found, session gone", id);
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

  StringBuffer getRequestUrl(HttpServletRequest request, Set<String> queryStringParamsToRemove) {
    StringBuffer requestUrl = request.getRequestURL();
    String qs = request.getQueryString();
    if (qs != null) {
      String qsSeparator = "?";
      for (String paramArg : Splitter.on("&").split(qs)) {
        String[] paramArgArr = paramArg.split("=", 2);
        if (!queryStringParamsToRemove.contains(paramArgArr[0])) {
          requestUrl.append(qsSeparator).append(paramArg);
          qsSeparator = "&";
        }
      }
    }
    return requestUrl;
  }

  String getRequestUrl(HttpServletRequest request) {
    return getRequestUrl(request, Collections.<String>emptySet()).toString();
  }

  String getRequestUrlWithoutToken(HttpServletRequest request) {
    return getRequestUrl(request, TOKEN_PARAM_SET).toString();
  }

  /*
   * Creates Login url with request URL as parameter for a callback redirection on a successful login.
   */
  String getLoginUrl(HttpServletRequest request) {
    String requestUrl = getRequestUrl(request, Collections.<String>emptySet()).toString();
    return ssoService.createRedirectToLoginURL(requestUrl);
  }

  /*
   * Removes the token from request URL, redirects to the modified URL, returns the token as a header
   */
  Authentication redirectToSelf(HttpServletRequest httpReq, HttpServletResponse httpRes) throws ServerAuthException {
    String authToken = httpReq.getParameter(SSOConstants.USER_AUTH_TOKEN_PARAM);
    String urlWithoutToken = getRequestUrlWithoutToken(httpReq);
    httpRes.setHeader(SSOConstants.X_USER_AUTH_TOKEN, authToken);
    try {
      LOG.debug("Redirecting to self without token '{}'", urlWithoutToken);
      httpRes.sendRedirect(urlWithoutToken);
      return Authentication.SEND_CONTINUE;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", urlWithoutToken, ex.toString(), ex));
    }
  }

  Authentication redirectToLogin(HttpServletRequest httpReq, HttpServletResponse httpRes) throws ServerAuthException {
    String urlToLogin = getLoginUrl(httpReq);
    try {
      LOG.debug("Redirecting to login '{}'", urlToLogin);
      httpRes.sendRedirect(urlToLogin);
      return Authentication.SEND_CONTINUE;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", urlToLogin, ex.toString(), ex));
    }
  }

  /*
   * Terminates the request with an HTTP forbbiden response
   */
  Authentication returnForbidden(HttpServletRequest httpReq, HttpServletResponse httpRes, String logMessageTemplate)
      throws ServerAuthException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(logMessageTemplate, getRequestUrl(httpReq));
    }
    try {
      httpRes.sendError(HttpServletResponse.SC_FORBIDDEN);
      if (httpReq.getHeader(SSOConstants.X_REST_CALL) != null) {
        httpRes.setContentType("application/json");
        httpRes.getWriter().println(FORBIDDEN_JSON_STR);
      }
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could send a FORBIDDEN (403) response: {}", ex.toString(), ex));
    }
    return Authentication.SEND_FAILURE;
  }

  Authentication handleRequestWithoutUserAuthHeader(HttpServletRequest httpReq, HttpServletResponse httpRes)
      throws ServerAuthException {
    Authentication ret;
    if ("GET".equals(httpReq.getMethod())) {
      String authTokenAsParam = httpReq.getParameter(SSOConstants.USER_AUTH_TOKEN_PARAM);
      if (authTokenAsParam != null) {
        // callback from login service, moving the token from the query string to a header via a self redirection

        // process the token and cache it if valid
        processToken(authTokenAsParam, httpReq, httpRes);

        // force a redirection to get rid of the tokne from the URL, it is injected as a response header.
        ret = redirectToSelf(httpReq, httpRes);
      } else {
        ret = handleRequiresAuthentication(httpReq, httpRes);
      }
    } else {
      // this is not a GET call, thus a a REST call

      ret = returnForbidden(httpReq, httpRes, "Unauthenticated REST request '{}'");
    }
    return ret;
  }

  Authentication handleRequiresAuthentication(HttpServletRequest httpReq, HttpServletResponse httpRes)
      throws ServerAuthException {
    Authentication ret;
    if (httpReq.getHeader(SSOConstants.X_REST_CALL) == null) {
      // this is a page request, redirecting to login service page

      ret = redirectToLogin(httpReq, httpRes);
    } else {
      // this is a REST call

      ret = returnForbidden(httpReq, httpRes, "Unauthenticated REST request '{}'");
    }
    return ret;
  }

  void registerToken(HttpSession session, SSOAuthenticationUser user) {
    knownTokens.put(user, session);
  }

  boolean isKnownToken(SSOAuthenticationUser user) {
    return knownTokens.containsKey(user);
  }

  Authentication processToken(String authToken, HttpServletRequest httpReq, HttpServletResponse httpRes)
      throws ServerAuthException {
    Authentication ret;
    try {
      if (ssoService.getTokenParser() != null) {
        SSOUserPrincipal userToken = ssoService.getTokenParser().parse(authToken);
        if (userToken != null) {
          SSOAuthenticationUser user = new SSOAuthenticationUser(userToken);
          LOG.debug("Token '{}' valid, user '{}'", authToken, user.getToken().getName());

          // caching the authenticated user info
          HttpSession session = httpReq.getSession(true);
          setAuthenticationUser(session, user);

          // registering crossreference to be able to invalidate the session if the token is invalidated
          registerToken(session, user);

          ret = user;
        } else {
          ret = returnForbidden(httpReq, httpRes, "Request '{}', token not valid");
        }
      } else {
        ret = returnForbidden(httpReq, httpRes, "Request '{}', token cannot be validated");
      }
    } catch (Exception ex) {
      LOG.warn("ClientUserToken verification failed: {}", ex.toString(), ex);
      ret = returnForbidden(httpReq, httpRes, "Request '{}' with invalid token");
    }
    return ret;
  }

  void setAuthenticationUser(HttpSession session, SSOAuthenticationUser user) {
    session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, user);
  }

  SSOAuthenticationUser getAuthenticationUser(HttpSession session) {
    return (session != null)
        ? (SSOAuthenticationUser) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED)
        : null;
  }

  boolean isLogoutRequest( HttpServletRequest httpReq) {
    String logoutPath = httpReq.getContextPath() + "/logout";
    return httpReq.getMethod().equals("POST") && httpReq.getRequestURI().equals(logoutPath);
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    ssoService.refresh();
    HttpSession session = httpReq.getSession(false);
    SSOAuthenticationUser user = getAuthenticationUser(session);
    Authentication ret = user;
    if (ret != null) {
      // cached token found

      // if authentication header is present, verify it matches the cached token
      String authToken = httpReq.getHeader(SSOConstants.X_USER_AUTH_TOKEN);
      if (authToken != null && !authToken.equals(user.getToken().getTokenStr())) {
        LOG.debug(
            "Request authentication token does not match cached token for '{}', re-validating",
            user.getToken().getName()
        );
        invalidateToken(user.getToken().getTokenId());
        ret = null;
      }
    }

    if (isLogoutRequest(httpReq)) {
      // trapping logout requests to return always OK
      if (ret != null) {
        LOG.debug("Logout request for '{}'", user.getToken().getName());
        invalidateToken(user.getToken().getTokenId());
      }
      httpRes.setStatus(HttpServletResponse.SC_OK);
      ret = Authentication.SEND_SUCCESS;
    } else {
      if (ret != null) {
        // cached token matches authentication header
        if (!(user).isValid()) {
          // cached token is invalid, invalidate session and forget token to trigger new authentication

          LOG.debug("User '{}' authentication token '{}' expired", user.getToken().getName(),
              user.getToken().getTokenStr()
          );
          session.invalidate();

          ret = handleRequiresAuthentication(httpReq, httpRes);
        } else {
          // cached token valid, do nothing

          if (LOG.isDebugEnabled()) {
            LOG.debug("Request '{}' cached authentication '{}'",
                getRequestUrl(httpReq),
                ((SSOAuthenticationUser) ret).getToken().getName()
            );
          }
        }
      } else {
        if (mandatory) {
          // the request needs authentication

          String authToken = httpReq.getHeader(SSOConstants.X_USER_AUTH_TOKEN);
          if (authToken == null) {
            // token not present in header

            ret = handleRequestWithoutUserAuthHeader(httpReq, httpRes);
          } else {
            // token present in header, REST call

            ret = processToken(authToken, httpReq, httpRes);
          }
        } else {
          // doing nothing, the request does not need authentication

          if (LOG.isDebugEnabled()) {
            LOG.debug("Request '{}' does not require authentication", getRequestUrl(httpReq));
          }
          ret = Authentication.NOT_CHECKED;
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
