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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;

public class DisconnectedSSOService extends AbstractSSOService {
  private final DisconnectedAuthentication authentication;
  private volatile boolean enabled;

  public DisconnectedSSOService(DisconnectedAuthentication authentication) {
    this.authentication = authentication;
  }

  @Override
  public void setConfiguration(Configuration conf) {
    super.setConfiguration(conf);
    setLoginPageUrl(DisconnectedLoginServlet.URL_PATH);
    setLogoutUrl(DisconnectedLogoutServlet.URL_PATH);
  }

  @Override
  protected SSOPrincipal validateUserTokenWithSecurityService(String authToken) throws ForbiddenException {
    Utils.checkState(isEnabled(), "Disconnected mode not enabled");
    SSOPrincipal principal = authentication.getSessionHandler().get(authToken);
    if (principal == null) {
      Map error = ImmutableMap.of("message", Utils.format("Unrecognized token '{}'", SSOUtils.tokenForLog(authToken)));
      throw new ForbiddenException(error);
    }
    return principal;
  }

  @Override
  protected SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId)
      throws ForbiddenException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void register(Map<String, String> attributes) {
    authentication.reset();
    clearCaches();
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

}
