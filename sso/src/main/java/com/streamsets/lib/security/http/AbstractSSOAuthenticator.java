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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public abstract class AbstractSSOAuthenticator implements Authenticator {
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static final Map UNAUTHORIZED_JSON = ImmutableMap.of("ISSUES",
      ImmutableList.of(ImmutableMap.of("code", "SSO_01", "message", "User not authenticated"))
  );

  private final SSOService ssoService;

  public AbstractSSOAuthenticator(SSOService ssoService) {
    this.ssoService = ssoService;
  }

  public SSOService getSsoService() {
    return ssoService;
  }

  protected abstract Logger getLog();

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

  @Override
  public boolean secureResponse(
      ServletRequest request, ServletResponse response, boolean mandatory, Authentication.User validatedUser
  ) throws ServerAuthException {
    return true;
  }

  String getRequestInfoForLogging(HttpServletRequest request, String principalId) {
    StringBuffer requestUrl = request.getRequestURL();
    if (request.getQueryString() != null) {
      requestUrl.append("?<QUERY_STRING>");
    }
    String method = request.getMethod();
    String remoteAddress = SSOPrincipalUtils.getClientIpAddress(request);
    return "Address: " + remoteAddress + " Principal: " + principalId + " " + method + " " + requestUrl;
  }

  /*
   * Terminates the request with an HTTP Unauthorized response
   */
  protected Authentication returnUnauthorized(
      HttpServletRequest httpReq, HttpServletResponse httpRes, String principalId, String logMessageTemplate
  ) throws ServerAuthException {
    return returnUnauthorized(httpReq, httpRes, UNAUTHORIZED_JSON, principalId, logMessageTemplate);
  }

  protected Authentication returnUnauthorized(
      HttpServletRequest httpReq,
      HttpServletResponse httpRes,
      Map errorReason,
      String principalId,
      String logMessageTemplate
  ) throws ServerAuthException {
    if (getLog().isDebugEnabled()) {
      getLog().debug(logMessageTemplate, getRequestInfoForLogging(httpReq, principalId));
    }
    try {
      httpRes.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), "dpm");
      httpRes.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      httpRes.setContentType("application/json");
      OBJECT_MAPPER.writeValue(httpRes.getWriter(), errorReason);
      return Authentication.SEND_FAILURE;
    } catch (IOException ex) {
      throw new ServerAuthException(Utils.format("Could send a Unauthorized (401) response: {}", ex.toString(), ex));
    }
  }

}
