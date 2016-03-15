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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SSOUserAuthenticator implements Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOUserAuthenticator.class);

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

  private final Cache<String, SSOAuthenticationUser> knownTokens;

  public SSOUserAuthenticator(String appContext, SSOService ssoService) {
    LOG.debug("App context '{}' using SSO for users '{}'", appContext, ssoService);
    this.ssoService = ssoService;

    // register a listener with the SSOService to listen to invalidations from the security service
    ssoService.setListener(new SSOService.Listener() {
      @Override
      public void invalidate(List<String> tokenIds) {
        SSOUserAuthenticator.this.invalidate(tokenIds);
      }
    });

    knownTokens = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.MINUTES).build();
  }

  void registerToken(String authToken, SSOAuthenticationUser user) {
    knownTokens.put(authToken, user);
  }

  SSOAuthenticationUser getFromCache(String token) {
    return knownTokens.getIfPresent(token);
  }

  boolean isKnownToken(String token) {
    return getFromCache(token) != null;
  }

  void invalidate(List<String> tokenIds) {
    for (String id : tokenIds) {
      invalidateToken(id);
    }
  }

  void invalidateToken(String id) {
    if (isKnownToken(id)) {
      knownTokens.invalidate(id);
      LOG.debug("Token '{}' invalidated", id);
    } else {
      LOG.debug("Token '{}' not found", id);
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
      httpRes.addCookie(createAuthCookie(httpReq, "", 0));
      if (httpReq.getHeader(SSOConstants.X_REST_CALL) != null) {
        httpRes.sendError(HttpServletResponse.SC_FORBIDDEN);
        httpRes.setContentType("application/json");
        httpRes.getWriter().println(FORBIDDEN_JSON_STR);
      } else {
        redirectToLogin(httpReq, httpRes);
      }
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not send a FORBIDDEN (403) response: {}", ex.toString(), ex));
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

  Cookie createAuthCookie(HttpServletRequest httpReq, String authToken, int secondsToLive) {
    Cookie authCookie = new Cookie(getAuthCookieName(httpReq), authToken);
    authCookie.setPath("/");
    authCookie.setMaxAge(secondsToLive);
    authCookie.setSecure(httpReq.isSecure());
    return authCookie;
  }

  Authentication processToken(String authToken, HttpServletRequest httpReq, HttpServletResponse httpRes)
      throws ServerAuthException {
    Authentication ret;
    try {
      if (ssoService.getTokenParser() != null) {
        SSOUserPrincipal userToken = ssoService.getTokenParser().parse(authToken);
        if (userToken != null) {
          SSOAuthenticationUser user = new SSOAuthenticationUser(userToken);
          LOG.debug("Token '{}' valid, user '{}'", authToken, user.getSSOUserPrincipal().getName());

          // caching the authenticated user info
          knownTokens.put(authToken, user);
          httpRes.addCookie(createAuthCookie(
              httpReq,
              authToken,
              (int) (user.getSSOUserPrincipal().getExpires() - System.currentTimeMillis()) / 1000)
          );
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

  boolean isLogoutRequest( HttpServletRequest httpReq) {
    String logoutPath = httpReq.getContextPath() + "/logout";
    return httpReq.getMethod().equals("POST") && httpReq.getRequestURI().equals(logoutPath);
  }

  String getAuthCookieName(HttpServletRequest httpReq) {
    return SSOConstants.AUTHENTICATION_COOKIE_PREFIX + httpReq.getServerPort();
  }

  String getAuthTokenFromRequest(HttpServletRequest httpReq) {
    String authToken = httpReq.getHeader(SSOConstants.X_USER_AUTH_TOKEN);
    if (authToken == null) {
      Cookie[] cookies = httpReq.getCookies();
      if (cookies != null) {
        for (Cookie cookie : cookies) {
          if (cookie.getName().equals(getAuthCookieName(httpReq))) {
            authToken = cookie.getValue();
            break;
          }
        }
      }
    }
    return authToken;
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    ssoService.refresh();
    String authToken = getAuthTokenFromRequest(httpReq);
    SSOAuthenticationUser user = (authToken == null) ? null : getFromCache(authToken);
    Authentication ret = user;
    if (isLogoutRequest(httpReq)) {
      // trapping logout requests to return always OK
      if (ret != null) {
        LOG.debug("Logout request for '{}'", user.getSSOUserPrincipal().getName());
        invalidateToken(user.getSSOUserPrincipal().getTokenId());
      }
      httpRes.setStatus(HttpServletResponse.SC_OK);
      ret = Authentication.SEND_SUCCESS;
    } else {
      if (ret != null) {
        // cached token matches authentication header
        if (!(user).isValid()) {
          // cached token is invalid, invalidate cache and delete auth cookie
          invalidateToken(authToken);

          LOG.debug("User '{}' authentication token '{}' expired", user.getSSOUserPrincipal().getName(),
              user.getSSOUserPrincipal().getTokenStr()
          );
          httpRes.addCookie(createAuthCookie(httpReq, "", 0));

          ret = handleRequiresAuthentication(httpReq, httpRes);
        } else {
          // cached token valid, do nothing

          if (LOG.isDebugEnabled()) {
            LOG.debug("Request '{}' cached authentication '{}'",
                getRequestUrl(httpReq),
                ((SSOAuthenticationUser) ret).getSSOUserPrincipal().getName()
            );
          }
        }
      } else {
        if (mandatory) {
          // the request needs authentication

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
