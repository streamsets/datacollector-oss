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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
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
import java.util.Set;

public class SSOUserAuthenticator extends AbstractSSOAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOUserAuthenticator.class);

  private final static Set<String> TOKEN_PARAM_SET =
      ImmutableSet.of(SSOConstants.USER_AUTH_TOKEN_PARAM, SSOConstants.REPEATED_REDIRECT_PARAM);

  public static final String HTTP_LOAD_BALANCER_URL = "http.load.balancer.url";

  private Configuration conf;
  private String loadBalancerURL;
  private boolean loadBalancerSecure;

  public SSOUserAuthenticator(SSOService ssoService, Configuration conf) {
    super(ssoService);
    this.conf = conf;
    if (this.conf != null) {
      this.loadBalancerURL = conf.get(HTTP_LOAD_BALANCER_URL, null);
      if (this.loadBalancerURL != null && this.loadBalancerURL.trim().toLowerCase().startsWith("https")) {
        this.loadBalancerSecure = true;
      }
    }
  }

  @Override
  protected Logger getLog() {
    return LOG;
  }

  StringBuffer getRequestUrl(HttpServletRequest request, Set<String> queryStringParamsToRemove) {
    StringBuffer requestUrl;

    if (this.loadBalancerURL != null) {
      requestUrl = new StringBuffer(this.loadBalancerURL);
      requestUrl.append(request.getRequestURI());
    } else {
      requestUrl = new StringBuffer(request.getRequestURL());
    }

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
  String getLoginUrl(HttpServletRequest request, boolean repeatedRedirect) {
    String requestUrl = getRequestUrl(request, TOKEN_PARAM_SET).toString();
    return getSsoService().createRedirectToLoginUrl(requestUrl, repeatedRedirect);
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
    boolean repeatedRedirect = httpReq.getParameter(SSOConstants.REPEATED_REDIRECT_PARAM) != null;
    String urlToLogin = getLoginUrl(httpReq, repeatedRedirect);
    try {
      LOG.debug("Redirecting to login '{}'", urlToLogin);
      httpRes.sendRedirect(urlToLogin);
      return Authentication.SEND_CONTINUE;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", urlToLogin, ex.toString(), ex));
    }
  }

  Authentication redirectToLogout(HttpServletResponse httpRes) throws ServerAuthException {
    String urlToLogout = getSsoService().getLogoutUrl();
    try {
      LOG.debug("Redirecting to logout '{}'", urlToLogout);
      httpRes.sendRedirect(urlToLogout);
      return Authentication.SEND_SUCCESS;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", urlToLogout, ex.toString(), ex));
    }
  }

  /*
   * Terminates the request with an HTTP forbbidden response or redirects to login for page requests
   */
  @Override
  protected Authentication returnForbidden(
      HttpServletRequest httpReq, HttpServletResponse httpRes, String principalId, String logMessageTemplate
  ) throws ServerAuthException {
    Authentication ret;
    if (httpReq.getHeader(SSOConstants.X_REST_CALL) != null) {
      ret = super.returnForbidden(httpReq, httpRes, null, logMessageTemplate);
    } else {
      redirectToLogin(httpReq, httpRes);
      ret = Authentication.SEND_FAILURE;
    }
    return ret;
  }

  Cookie createAuthCookie(HttpServletRequest httpReq, String authToken, int secondsToLive) {
    Cookie authCookie = new Cookie(getAuthCookieName(httpReq), authToken);
    authCookie.setPath("/");
    authCookie.setMaxAge(secondsToLive);
    authCookie.setSecure(httpReq.isSecure() || loadBalancerSecure);
    return authCookie;
  }

  void setAuthCookieIfNecessary(HttpServletRequest req, HttpServletResponse res, String authToken, long expiresMillis) {
    if (!authToken.equals(getAuthTokenFromCookie(req))) {
      res.addCookie(createAuthCookie(req, authToken, (int) (expiresMillis - System.currentTimeMillis()) / 1000));
    }
  }

  boolean isLogoutRequest( HttpServletRequest httpReq) {
    String logoutPath = httpReq.getContextPath() + "/logout";
    return httpReq.getMethod().equals("GET") && httpReq.getRequestURI().equals(logoutPath);
  }

  private boolean isCORSOptionsRequest(HttpServletRequest httpReq) {
    return "OPTIONS".equals(httpReq.getMethod());
  }

  String getAuthCookieName(HttpServletRequest httpReq) {
    return SSOConstants.AUTHENTICATION_COOKIE_PREFIX + httpReq.getServerPort();
  }

  String getAuthTokenFromCookie(HttpServletRequest httpReq) {
    String authToken = null;
    Cookie[] cookies = httpReq.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(getAuthCookieName(httpReq))) {
          authToken = cookie.getValue();
          break;
        }
      }
    }
    return authToken;
  }

  boolean isAuthTokenInQueryString(HttpServletRequest httpReq) {
    return httpReq.getParameter(SSOConstants.USER_AUTH_TOKEN_PARAM) != null;
  }

  String getAuthTokenFromRequest(HttpServletRequest httpReq) {
    String authToken = httpReq.getParameter(SSOConstants.USER_AUTH_TOKEN_PARAM);
    if (authToken == null) {
      authToken = httpReq.getHeader(SSOConstants.X_USER_AUTH_TOKEN);
      if (authToken == null) {
        authToken = getAuthTokenFromCookie(httpReq);
      }
    }
    return authToken;
  }

  String getAuthTokenForLogging(String authToken) {
    // if less than 16 then is invalid for sure, we can fully show it
    if (authToken == null) {
      authToken = "null";
    }
    String fragment = (authToken.length() < 16) ? authToken : (authToken.substring(0, 16) + "...");
    return "TOKEN:" + fragment;
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    String authToken = getAuthTokenFromRequest(httpReq);

    Authentication ret;

    if (LOG.isTraceEnabled()) {
      LOG.trace("Request: {}", getRequestInfoForLogging(httpReq, getAuthTokenForLogging(authToken)));
    }

    if (isCORSOptionsRequest(httpReq)) {
      httpRes.setStatus(HttpServletResponse.SC_OK);
      httpRes.setHeader("Access-Control-Allow-Origin", conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN,
          CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT));
      httpRes.setHeader("Access-Control-Allow-Headers", conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS,
          CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT));
      httpRes.setHeader("Access-Control-Allow-Methods", conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS,
          CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT));
      return Authentication.SEND_SUCCESS;
    }

    if (!mandatory) {
      ret = Authentication.NOT_CHECKED;
    } else {
      getSsoService().refresh();
      SSOUserPrincipal principal = null;
      if (authToken != null) {
        principal = getSsoService().validateUserToken(authToken);
      }
      if (principal != null) {
        SSOAuthenticationUser user = new SSOAuthenticationUser(principal);
        if (isLogoutRequest(httpReq)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Principal '{}' Logout", principal.getPrincipalId());
          }
          getSsoService().invalidateUserToken(authToken);
          ret = redirectToLogout(httpRes);
        } else {
          setAuthCookieIfNecessary(httpReq, httpRes, authToken, user.getSSOUserPrincipal().getExpires());
          if (isAuthTokenInQueryString(httpReq)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "Redirection to self, principal '{}' request: {}",
                  principal.getPrincipalId(),
                  getRequestInfoForLogging(httpReq, getAuthTokenForLogging(authToken))
              );
            }
            ret = redirectToSelf(httpReq, httpRes);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Principal '{}' request: {}",
                  principal.getPrincipalId(),
                  getRequestInfoForLogging(httpReq, getAuthTokenForLogging(authToken))
              );
            }
            ret = user;
          }
        }
      } else {
        ret = returnForbidden(httpReq, httpRes, getAuthTokenForLogging(authToken), "Could not authenticate: {}");
      }
    }
    return ret;
  }

}
