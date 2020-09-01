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
import java.time.Instant;

public class AsterAuthenticator implements Authenticator {
  private static final Logger LOG = LoggerFactory.getLogger(AsterAuthenticator.class);

  private static final String AUTHENTICATION_METHOD = "aster-sso";
  private static final String HTTP_GET = "GET";
  private static final String T_RETRY_QS_PARAM = "t_retry";

  private final AsterService service;

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
    //NoOp
  }

  @Override
  public String getAuthMethod() {
    return AUTHENTICATION_METHOD;
  }

  @Override
  public void prepareRequest(ServletRequest servletRequest) {
  }

  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return true;
  }

  boolean isEngineRegistered() {
    return service.isEngineRegistered();
  }

  String getBaseUrlForRedirection(HttpServletRequest httpReq) throws ServerAuthException {
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

  String getUrlForRedirection(HttpServletRequest httpReq) throws ServerAuthException {
    String redirectionUrl;
    if (httpReq.getMethod().equals(HTTP_GET)) {
      String queryString = httpReq.getQueryString();
      queryString = (queryString == null) ? "" : "?" + queryString;
      redirectionUrl = httpReq.getRequestURL().toString() + queryString;
    } else {
      redirectionUrl = getBaseUrlForRedirection(httpReq);
    }
    return redirectionUrl;
  }

  String addParameterToQueryString(String redirectUrl, String param, String value) {
    return redirectUrl + ((redirectUrl.contains("?")) ? "&" : "?") + param + "=" + value;
  }

  Authentication redirect(HttpServletRequest httpReq, HttpServletResponse httpRes, String redirectUrl) throws
      ServerAuthException {
    try {
      httpRes.sendRedirect(redirectUrl);
      return Authentication.SEND_SUCCESS;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could not redirect to '{}': {}", redirectUrl, ex.toString(), ex));
    }
  }

  boolean isEngineRegistrationOrLoginPageWithInvalidState(HttpServletRequest httpReq) {
    boolean itIsInvalid = false;
    String requestUri = httpReq.getRequestURI();
    if (
        service.getConfig().getUserLoginPath().equals(requestUri) ||
        service.getConfig().getEngineRegistrationPath().equals(requestUri)
    ) {
      String lState = httpReq.getParameter(AsterService.LSTATE_QS_PARAM);
      itIsInvalid = lState == null || !service.isValidLocalState(lState);
    }
    return itIsInvalid;
  }

  boolean isHandlingEngineRegistration(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String requestUri = httpReq.getRequestURI();
    return service.getConfig().getRegistrationUrlRestPath().equals(requestUri);
  }

  boolean isHandlingUserLogin(HttpServletRequest httpReq, HttpServletResponse httpRes) {
    String requestUri = httpReq.getRequestURI();
    return service.getConfig().getUserLoginRestPath().equals(requestUri);
  }

  boolean shouldRetry(HttpServletRequest httpReq) {
    return httpReq.getMethod().equals(HTTP_GET) && !"false".equals(httpReq.getParameter(T_RETRY_QS_PARAM));
  }

  SSOPrincipal createPrincipal(AsterUser AsterUser) {
    SSOPrincipalJson principal = new SSOPrincipalJson();
    principal.setTokenStr("aster-token"); //we don't need/have a token
    principal.setPrincipalId(AsterUser.getName());
    principal.setOrganizationId(AsterUser.getOrg());
    principal.setEmail(AsterUser.getName());
    principal.getRoles().addAll(AsterUser.getRoles());
    principal.getGroups().addAll(AsterUser.getGroups());
    int expiresSecs = service.getConfig().getEngineConfig().get("http.session.max.inactive.interval", 86400);
    principal.setExpires(Instant.now().plusSeconds(expiresSecs).getEpochSecond());
    principal.lock();
    return principal;
  }

  Authentication getSessionAuthentication(HttpServletRequest httpReq) {
    HttpSession session = httpReq.getSession(false);
    return (session == null) ? null : (Authentication) session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);
  }

  void authenticateSession(HttpServletRequest httpReq, AsterUser user) {
    HttpSession session = httpReq.getSession(true);
    session.setAttribute(SessionAuthentication.__J_AUTHENTICATED, new SSOAuthenticationUser(createPrincipal(user)));
  }

  void destroySession(HttpServletRequest httpReq) {
    HttpSession session = httpReq.getSession(false);
    if (session != null) {
      session.invalidate();
    }
  }

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
      mandatory |= isEngineRegistrationOrLoginPageWithInvalidState(httpReq);
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
          } else {
            LOG.trace("User is not authenticated");
            if (isHandlingUserLogin(httpReq, httpRes)) {
              LOG.trace("Handling user login");
              AsterUser user = service.handleUserLogin(getBaseUrlForRedirection(httpReq), httpReq, httpRes);
              if (user == null) {
                authentication = Authentication.SEND_SUCCESS;
              } else {
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
            String redirUrl = service.handleEngineRegistration(getBaseUrlForRedirection(httpReq), httpReq, httpRes);
            if (redirUrl == null) {
              authentication = Authentication.SEND_SUCCESS;
            } else {
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
      if (shouldRetry(httpReq)) {
        String redirUrl = getUrlForRedirection(httpReq);
        LOG.debug("Retry authentication redirect '{}'", redirUrl);
        authentication = redirect(httpReq, httpRes, addParameterToQueryString(redirUrl, T_RETRY_QS_PARAM, "false"));
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
