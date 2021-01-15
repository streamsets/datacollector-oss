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

import com.streamsets.datacollector.util.Configuration;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

public class ProxySSOService implements SSOService {
  private SSOService ssoService;

  public ProxySSOService(SSOService ssoService) {
    this.ssoService = ssoService;
  }

  @Override
  public void setDelegateTo(SSOService ssoService) {
    this.ssoService = ssoService;
  }

  @Override
  public SSOService getDelegateTo() {
    return ssoService;
  }

  @Override
  public void setConfiguration(Configuration configuration) {
    ssoService.setConfiguration(configuration);
  }

  @Override
  public void register(Map<String, String> attributes) {
    ssoService.register(attributes);
  }

  @Override
  public String createRedirectToLoginUrl(
      String requestUrl, boolean duplicateRedirect, HttpServletRequest req, HttpServletResponse res
  ) {
    return ssoService.createRedirectToLoginUrl(requestUrl, duplicateRedirect, req, res);
  }

  @Override
  public String getLoginPageUrl() {
    return ssoService.getLoginPageUrl();
  }

  @Override
  public String getLogoutUrl() {
    return ssoService.getLogoutUrl();
  }

  @Override
  public boolean invalidateUserToken(String authToken) {
    return ssoService.invalidateUserToken(authToken);
  }

  @Override
  public SSOPrincipal validateUserToken(String authToken) {
    return ssoService.validateUserToken(authToken);
  }

  @Override
  public SSOPrincipal validateAppToken(String authToken, String componentId) {
    return ssoService.validateAppToken(authToken, componentId);
  }

  @Override
  public boolean invalidateAppToken(String authToken) {
    return ssoService.invalidateAppToken(authToken);
  }

  @Override
  public void clearCaches() {
    ssoService.clearCaches();
  }

  @Override
  public void setRegistrationResponseDelegate(RegistrationResponseDelegate delegate) {
    ssoService.setRegistrationResponseDelegate(delegate);
  }

}
