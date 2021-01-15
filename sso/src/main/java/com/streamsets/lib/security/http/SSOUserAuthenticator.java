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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.jetbrains.annotations.NotNull;
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

  private static final Set<String> TOKEN_PARAM_SET =
      ImmutableSet.of(SSOConstants.USER_AUTH_TOKEN_PARAM, SSOConstants.REPEATED_REDIRECT_PARAM);

  public static final String HTTP_META_REDIRECT_TO_SSO = "http.meta.redirect.to.sso";
  private static final String BASE_HTTP_URL_ATTR = "%s.base.http.url";

  private Configuration conf;
  private boolean doMetaRedirectToSso;
  private String dpmBaseUrl;
  private final boolean isDataCollector;
  private String engineBaseHttpUrl;

  public SSOUserAuthenticator(
      SSOService ssoService,
      @NotNull Configuration conf
  ) {
    this(ssoService, conf, null);
  }

  public SSOUserAuthenticator(
      SSOService ssoService,
      @NotNull Configuration conf,
      String productName
  ) {
    super(ssoService);
    this.conf = conf;

    this.dpmBaseUrl = conf.get(RemoteSSOService.DPM_BASE_URL_CONFIG, null);
    this.isDataCollector = !conf.hasName(RemoteSSOService.DPM_APP_SECURITY_URL_CONFIG);
    this.doMetaRedirectToSso = conf.get(HTTP_META_REDIRECT_TO_SSO, false);

    if (StringUtils.isNotEmpty(this.dpmBaseUrl) && this.dpmBaseUrl.endsWith("/")) {
      this.dpmBaseUrl = this.dpmBaseUrl.substring(0, this.dpmBaseUrl.length() - 1);
    }

    if (isDataCollector) {
      this.engineBaseHttpUrl = conf.get(String.format(BASE_HTTP_URL_ATTR, productName), null);
    }
  }

  @Override
  protected Logger getLog() {
    return LOG;
  }

  StringBuffer getRequestUrl(HttpServletRequest request, Set<String> queryStringParamsToRemove) {
    StringBuffer requestUrl;

    if (this.dpmBaseUrl != null && !isDataCollector) {
      requestUrl = new StringBuffer(this.dpmBaseUrl);
      requestUrl.append(request.getRequestURI());
    } else if (this.engineBaseHttpUrl != null && this.isDataCollector) {
      requestUrl = new StringBuffer(this.engineBaseHttpUrl);
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
    return getRequestUrl(request, Collections.emptySet()).toString();
  }

  String getRequestUrlWithoutToken(HttpServletRequest request) {
    return getRequestUrl(request, TOKEN_PARAM_SET).toString();
  }

  /*
   * Creates Login url with request URL as parameter for a callback redirection on a successful login.
   */
  String getLoginUrl(
      HttpServletRequest request,
      boolean repeatedRedirect,
      HttpServletRequest httpReq,
      HttpServletResponse httpRes
  ) {
    String requestUrl = getRequestUrl(request, TOKEN_PARAM_SET).toString();
    return getSsoService().createRedirectToLoginUrl(requestUrl, repeatedRedirect, httpReq, httpRes);
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

  @VisibleForTesting
  static final String HTML_META_REDIRECT =
      "<HTML><HEAD><meta http-equiv=\"refresh\" content=\"0; url='%s'\"/></HEAD></HTML>";

  Authentication redirectToLogin(HttpServletRequest httpReq, HttpServletResponse httpRes) throws ServerAuthException {
    boolean repeatedRedirect = httpReq.getParameter(SSOConstants.REPEATED_REDIRECT_PARAM) != null;
    String urlToLogin = getLoginUrl(httpReq, repeatedRedirect, httpReq, httpRes);
    try {
      LOG.debug("Redirecting to login '{}'", urlToLogin);
      if (doMetaRedirectToSso) {
        httpRes.setContentType("text/html");
        httpRes.setStatus(HttpServletResponse.SC_OK);
        httpRes.getWriter().println(String.format(HTML_META_REDIRECT, urlToLogin));
      } else {
        httpRes.sendRedirect(urlToLogin);
      }
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
   * Terminates the request with an HTTP Unauthorized response or redirects to login for page requests
   */
  @Override
  protected Authentication returnUnauthorized(
      HttpServletRequest httpReq, HttpServletResponse httpRes, String principalId, String logMessageTemplate
  ) throws ServerAuthException {
    Authentication ret;
    httpRes.addCookie(createAuthCookie(httpReq, "", 0));
    if (httpReq.getHeader(SSOConstants.X_REST_CALL) != null) {
      ret = super.returnUnauthorized(httpReq, httpRes, null, logMessageTemplate);
    } else {
      redirectToLogin(httpReq, httpRes);
      ret = Authentication.SEND_FAILURE;
    }
    return ret;
  }

  Cookie createAuthCookie(HttpServletRequest httpReq, String authToken, long expiresMillis) {
    Cookie authCookie = new Cookie(getAuthCookieName(httpReq), authToken);
    authCookie.setPath("/");
    // if positive it is a persistent session, else a transient one and we don't have to set the cookie age
    if (expiresMillis > 0) {
      int secondsToLive = (int) ((expiresMillis - System.currentTimeMillis()) / 1000);
      authCookie.setMaxAge(secondsToLive);
    } else if (expiresMillis == 0) {
      // to delete the cookie
      authCookie.setMaxAge(0);
    }


    if (isDataCollector) {
      // When an SDC is accessing SCH, set the cookie based on the SDC's scheme
      authCookie.setSecure(httpReq.isSecure());
    } else {
      // When a browser accesses SCH, set the cookie based on the SCH endpoint
      authCookie.setSecure(dpmBaseUrl.startsWith("https"));
    }

    return authCookie;
  }

  void setAuthCookieIfNecessary(HttpServletRequest req, HttpServletResponse res, String authToken, long expiresMillis) {
    if (!authToken.equals(getAuthTokenFromCookie(req))) {
      res.addCookie(createAuthCookie(req, authToken, expiresMillis));
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

  Cookie getAuthCookie(HttpServletRequest httpReq) {
    Cookie[] cookies = httpReq.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(getAuthCookieName(httpReq))) {
          return cookie;
        }
      }
    }
    return null;
  }

  String getAuthTokenFromCookie(HttpServletRequest httpReq) {
    Cookie cookie = getAuthCookie(httpReq);
    return (cookie == null) ? null : cookie.getValue();
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

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    String authToken = getAuthTokenFromRequest(httpReq);

    Authentication ret = null;

    if (LOG.isTraceEnabled()) {
      LOG.trace("Request: {}", getRequestInfoForLogging(httpReq, SSOUtils.tokenForLog(authToken)));
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
      if (authToken != null) {
        try {
          SSOPrincipal principal = getSsoService().validateUserToken(authToken);
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
                      getRequestInfoForLogging(httpReq, SSOUtils.tokenForLog(authToken))
                  );
                }
                ret = redirectToSelf(httpReq, httpRes);
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      "Principal '{}' request: {}",
                      principal.getPrincipalId(),
                      getRequestInfoForLogging(httpReq, SSOUtils.tokenForLog(authToken))
                  );
                }
                ret = user;
              }
            }
          }
        } catch (ForbiddenException fex) {
          ret = returnUnauthorized(httpReq, httpRes, fex.getErrorInfo(), null, "Request: {}");
        }
      }
    }
    if (ret == null) {
      ret = returnUnauthorized(httpReq, httpRes, SSOUtils.tokenForLog(authToken), "Could not authenticate: {}");
    }
    return ret;
  }

}
