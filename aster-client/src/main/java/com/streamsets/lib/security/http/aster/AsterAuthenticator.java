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
package com.streamsets.lib.security.http.aster;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.lib.security.http.CORSConstants;
import com.streamsets.lib.security.http.SSOAuthenticationUser;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.lib.security.http.SSOPrincipalJson;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.http.HttpHeader;
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
import java.net.URL;
import java.net.URLEncoder;
import java.time.Instant;

/**
 * Jetty {@link Authenticator} implementation for Aster SSO for engines
 */
public class AsterAuthenticator implements Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(AsterAuthenticator.class);

  private static final String AUTHENTICATION_METHOD = "aster-sso";
  private static final String HTTP_GET = "GET";
  private static final String A_RETRY_QS_PARAM = "a_retry";

  private final AsterService service;

  /**
   * Constructor.
   */
  public AsterAuthenticator(AsterService service) {
    this.service = service;
  }

  /**
   * Returns the Aster service.
   */
  protected AsterService getService() {
    return service;
  }

  /**
   * No-Op, configuration is provided via constructor.
   * @param authConfiguration
   */
  @Override
  public void setConfiguration(AuthConfiguration authConfiguration) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getAuthMethod() {
    return AUTHENTICATION_METHOD;
  }

  /**
   * No-Op.
   */
  @Override
  public void prepareRequest(ServletRequest servletRequest) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return true;
  }

  /**
   * Returns if the engine is register with Aster already.
   */
  @VisibleForTesting
  boolean isEngineRegistered() {
    return service.isEngineRegistered();
  }

  /**
   * Returns the engine base URL, {@code <scheme>://<host>:<port>}, from the request to create full URLs for redirect.
   */
  String getRequestBaseUrl(HttpServletRequest httpReq) throws ServerAuthException {
    try {
      String baseUrl;
      String url = new URL(httpReq.getRequestURL().toString()).toExternalForm();
      if (httpReq.getRequestURI().isEmpty()) {
        baseUrl = url;
      } else {
        baseUrl = url.substring(0, url.length() - httpReq.getRequestURI().length());
      }
      return baseUrl;
    } catch (Exception ex) {
      throw new ServerAuthException("Could not get base URL from request: " + ex.getMessage(), ex);
    }
  }

  /**
   * Returns the full redirect URL for the current request.
   * If it is not a GET request, it returns the the engine base URL.
   */
  @VisibleForTesting
  String getUrlForRedirection(HttpServletRequest httpReq) throws ServerAuthException {
    String redirectionUrl;
    if (httpReq.getMethod().equals(HTTP_GET)) {
      String queryString = httpReq.getQueryString();
      queryString = (queryString == null) ? "" : "?" + queryString;
      redirectionUrl = httpReq.getRequestURL().toString() + queryString;
    } else {
      redirectionUrl = getRequestBaseUrl(httpReq);
    }
    return redirectionUrl;
  }

  /**
   * Adds a query string parameter the given URL, adding {@code ?} or {@code &} depending if there is not a
   * query string the given URL. The value is URL encoded.
   */
  @VisibleForTesting
  String addParameterToQueryString(String redirectUrl, String param, String value) {
    return redirectUrl + ((redirectUrl.contains("?")) ? "&" : "?") + param + "=" + URLEncoder.encode(value);
  }

  /**
   * Triggers a redirect as response.
   */
  @VisibleForTesting
  Authentication redirect(HttpServletRequest httpReq, HttpServletResponse httpRes, String redirectUrl) throws
      ServerAuthException {
    try {
      httpRes.sendRedirect(redirectUrl);
      return Authentication.SEND_SUCCESS;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", redirectUrl, ex.toString(), ex));
    }
  }

  /**
   * Returns if the request is for a registration or login page.
   */
  @VisibleForTesting
  boolean isEngineRegistrationOrLoginPage(HttpServletRequest httpReq) {
    String requestUri = httpReq.getRequestURI();
    return service.getConfig().getUserLoginPath().equals(requestUri) ||
        service.getConfig().getEngineRegistrationPath().equals(requestUri);
  }

  /**
   * Returns if the request is using a valid local state (still cached).
   */
  @VisibleForTesting
  boolean isValidState(HttpServletRequest httpReq) {
    String lState = httpReq.getParameter(AsterService.LSTATE_QS_PARAM);
    return lState != null && service.isValidLocalState(lState);
  }

  /**
   * Returns if the request is for handling engine registration.
   */
  @VisibleForTesting
  boolean isHandlingEngineRegistration(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String requestUri = httpReq.getRequestURI();
    return service.getConfig().getRegistrationUrlRestPath().equals(requestUri);
  }

  /**
   * Returns if the request is for handling user login.
   */
  @VisibleForTesting
  boolean isHandlingUserLogin(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String requestUri = httpReq.getRequestURI();
    return service.getConfig().getUserLoginRestPath().equals(requestUri);
  }

  /**
   * Returns if the failed request should be retried (to avoid redirection loops).
   */
  @VisibleForTesting
  boolean shouldRetryFailedRequest(HttpServletRequest httpReq) {
    return httpReq.getMethod().equals(HTTP_GET) && !"false".equals(httpReq.getParameter(A_RETRY_QS_PARAM));
  }

  /**
   * Creates an engine principal using the Aster user information received after a successful Aster SSO login.
   */
  @VisibleForTesting
  SSOPrincipal createPrincipal(AsterUser AsterUser) {
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setTokenStr("aster-token"); //we don't need/have a token
    principal.setPrincipalId(AsterUser.getName());
    principal.setOrganizationId(AsterUser.getOrg());
    principal.setEmail(AsterUser.getName());
    principal.getRoles().addAll(AsterUser.getRoles());
    principal.getGroups().addAll(AsterUser.getGroups());
    int expiresSecs = service.getConfig().getEngineConfig().get("http.session.max.inactive.interval", 86400);
    principal.setExpires(Instant.now().plusSeconds(expiresSecs).toEpochMilli());
    principal.lock();
    return principal;
  }

  /**
   * Returns the Jetty {@link Authentication} stored in the HTTP session.
   */
  @VisibleForTesting
  Authentication getSessionAuthentication(HttpServletRequest httpReq) {
    HttpSession session = httpReq.getSession(false);
    return (session == null) ? null : (Authentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);
  }

  /**
   * Creates and stores a Jetty {@link Authentication} in the HTTP session, making the session authenticated.
   */
  @VisibleForTesting
  void authenticateSession(HttpServletRequest httpReq, AsterUser user) {
    HttpSession session = httpReq.getSession(true);
    session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, new SSOAuthenticationUser(createPrincipal(user)));
  }

  /**
   * Terminates the HTTP session, un-authorizing the user.
   */
  @VisibleForTesting
  void destroySession(HttpServletRequest httpReq) {
    HttpSession session = httpReq.getSession(false);
    if (session != null) {
      session.invalidate();
    }
  }

  /**
   * Returns if the current request is a logout request.
   * @param httpReq
   * @return
   */
  @VisibleForTesting
  boolean isLogoutRequest( HttpServletRequest httpReq) {
    String logoutPath = httpReq.getContextPath() + "/rest/v1/authentication/logout";
    return httpReq.getMethod().equals("POST") && httpReq.getRequestURI().equals(logoutPath);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory) throws
      ServerAuthException {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;

    // CORS options
    if ("OPTIONS".equals(httpReq.getMethod())) {
      httpRes.setStatus(HttpServletResponse.SC_OK);
      httpRes.setHeader("Access-Control-Allow-Origin",
          service.getConfig()
              .getEngineConfig()
              .get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN,
                  CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT
              )
      );
      httpRes.setHeader("Access-Control-Allow-Headers",
          service.getConfig().getEngineConfig().get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS,
              CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT
          )
      );
      httpRes.setHeader("Access-Control-Allow-Methods",
          service.getConfig().getEngineConfig().get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS,
              CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT
          )
      );
      return Authentication.SEND_SUCCESS;
    }

    Authentication authentication;

    try {
      mandatory |= isEngineRegistrationOrLoginPage(httpReq) && !isValidState(httpReq);
      if (!mandatory) {
        authentication = Authentication.NOT_CHECKED;
      } else {
        if (isEngineRegistered()) {
          LOG.trace("Engine is registered");
          authentication = getSessionAuthentication(httpReq);
          if (authentication != null) {
            LOG.trace(
                "Authenticated user '{}'",
                ((SSOAuthenticationUser)authentication).getSSOUserPrincipal().getName()
            );
            if (isLogoutRequest(httpReq)) {
              LOG.trace(
                  "Logout user '{}'",
                  ((SSOAuthenticationUser)authentication).getSSOUserPrincipal().getName()
              );
              destroySession(httpReq);
              httpRes.sendRedirect("/tlogout.html");
              authentication = Authentication.SEND_SUCCESS;
            }
          } else {
            LOG.trace("User is not authenticated");
            if (isHandlingUserLogin(httpReq, httpRes)) {
              LOG.trace("Handling user login");
              AsterUser user = service.handleUserLogin(getRequestBaseUrl(httpReq), httpReq, httpRes);
              if (user == null) { //GET initiate
                authentication = Authentication.SEND_SUCCESS;
              } else { //POST complete
                authenticateSession(httpReq, user);
                authentication = redirect(httpReq, httpRes, user.getPreLoginUrl());
              }
            } else {
              LOG.trace("Triggering user login");
              destroySession(httpReq);
              String redirUrlKey = service.storeRedirUrl(getUrlForRedirection(httpReq));
              authentication = redirect(httpReq,
                  httpRes,
                  addParameterToQueryString(service.getConfig().getUserLoginPath(),
                      AsterService.LSTATE_QS_PARAM,
                      redirUrlKey
                  )
              );
            }
          }
        } else {
          LOG.trace("Engine is not registered");
          if (isHandlingEngineRegistration(httpReq, httpRes)) {
            LOG.trace("Handling engine registration");
            String redirUrl = service.handleEngineRegistration(getRequestBaseUrl(httpReq), httpReq, httpRes);
            if (redirUrl == null) { //GET initiate
              authentication = Authentication.SEND_SUCCESS;
            } else { //POST complete
              authentication = redirect(httpReq, httpRes, redirUrl);
            }
          } else {
            LOG.trace("Triggering engine registration");
            destroySession(httpReq);
            String redirUrlKey = service.storeRedirUrl(getUrlForRedirection(httpReq));
            authentication = redirect(httpReq,
                httpRes,
                addParameterToQueryString(service.getConfig().getEngineRegistrationPath(),
                    AsterService.LSTATE_QS_PARAM,
                    redirUrlKey
                )
            );
          }
        }
      }
    } catch (AsterException|AsterAuthException ex) {
      LOG.warn("Error during request authentication: {}", ex, ex);
      destroySession(httpReq);
      if (shouldRetryFailedRequest(httpReq)) {
        String redirUrl = getUrlForRedirection(httpReq);
        LOG.debug("Retry authentication redirect '{}'", redirUrl);
        authentication = redirect(httpReq, httpRes, addParameterToQueryString(redirUrl, A_RETRY_QS_PARAM, "false"));
      } else {
        LOG.warn("Already retried authentication, giving up");
        if (ex instanceof AsterAuthException) {
          httpRes.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), "engine-aster-sso");
          httpRes.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          authentication = Authentication.SEND_FAILURE;
        } else {
          httpRes.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          authentication = Authentication.SEND_FAILURE;
        }
      }
    } catch (Exception ex) {
      LOG.error("Error during request authentication: {}", ex, ex);
      httpRes.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      authentication = Authentication.SEND_FAILURE;
    }
    return authentication;
  }

}
