/**
 * Copyright 2016 StreamSets Inc.
 * <p/>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.lib.security.http;

import com.streamsets.datacollector.util.Configuration;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

public class SSOAuthenticator extends AbstractSSOAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(SSOAuthenticator.class);

  private final SSOUserAuthenticator userAuthenticator;
  private final SSOAppAuthenticator appAuthenticator;

  public SSOAuthenticator(String appContext, SSOService ssoService, Configuration configuration) {
    super(ssoService);
    userAuthenticator = new SSOUserAuthenticator(getSsoService(), configuration);
    appAuthenticator = new SSOAppAuthenticator(getSsoService());
  }

  @Override
  protected Logger getLog() {
    return LOG;
  }

  @Override
  public Authentication validateRequest(ServletRequest request, ServletResponse response, boolean mandatory)
      throws ServerAuthException {
    Authenticator auth = userAuthenticator;
    if (getSsoService().isAppAuthenticationEnabled()) {
      HttpServletRequest httpReq = (HttpServletRequest) request;
      boolean isRestCall = httpReq.getHeader(SSOConstants.X_REST_CALL) != null;
      boolean isAppCall = httpReq.getHeader(SSOConstants.X_APP_AUTH_TOKEN) != null ||
          httpReq.getHeader(SSOConstants.X_APP_COMPONENT_ID) != null;
      if (isAppCall && isRestCall) {
        auth = appAuthenticator;
        if (getLog().isTraceEnabled()) {
          getLog().trace("App request '{}'", getRequestInfoForLogging(httpReq, "?"));
        }
      } else {
        if (getLog().isTraceEnabled()) {
          getLog().trace("User request '{}'", getRequestInfoForLogging(httpReq, "?"));
        }
      }
    }
    return auth.validateRequest(request, response, mandatory);
  }

}
