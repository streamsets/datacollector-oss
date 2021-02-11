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

import com.streamsets.pipeline.api.impl.Utils;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public class SSOAppAuthenticator extends AbstractSSOAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOAppAuthenticator.class);

  public SSOAppAuthenticator(SSOService ssoService) {
    super(ssoService);
  }

  @Override
  protected Logger getLog() {
    return LOG;
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
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpRes = (HttpServletResponse) response;
    Authentication ret;
    String componentId = getAppComponentId(httpReq);
    if (!mandatory) {
      if (LOG.isDebugEnabled()) {
        LOG.trace("URL '{}' does not require authentication", getRequestInfoForLogging(httpReq, componentId));
      }
      ret = Authentication.NOT_CHECKED;
    } else {
      if (((HttpServletRequest) request).getHeader(SSOConstants.X_REST_CALL) == null) {
        ret = returnUnauthorized(httpReq, httpRes, componentId, "Not a REST call: {}");
      } else {
        String authToken = getAppAuthToken(httpReq);
        if (authToken == null) {
          ret = returnUnauthorized(httpReq, httpRes, componentId, "Missing app authentication token: {}");
        } else if (componentId == null) {
          ret = returnUnauthorized(httpReq, httpRes, null, "Missing component ID: {}");
        } else {
          try {
            SSOPrincipal principal = getSsoService().validateAppToken(authToken, componentId);
            if (principal != null) {
              ret = new SSOAuthenticationUser(principal);
            } else {
              ret = returnUnauthorized(httpReq, httpRes, componentId, "Invalid app authentication token: {}");
            }
          } catch (ForbiddenException fex) {
            ret = returnUnauthorized(httpReq, httpRes, fex.getErrorInfo(), componentId, "Request: {}");
          } catch (MovedException mex) {
            ret = returnMoved(httpReq, httpRes, mex);
          }
        }
      }
    }
    return ret;
  }

  protected Authentication returnMoved(HttpServletRequest httpReq, HttpServletResponse httpRes, MovedException mex) {
    httpRes.setHeader(HttpHeader.LOCATION.asString(), mex.getNewUrl(httpReq));
    httpRes.setStatus(308);
    return Authentication.SEND_FAILURE;
  }


}
