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
import java.util.List;
import java.util.Map;

public interface SSOService {
  String SSO_SERVICE_KEY = "ssoService";

  interface Listener {

    void invalidate(List<String> tokenStr);

  }

  void setDelegateTo(SSOService ssoService);

  SSOService getDelegateTo();

  void setConfiguration(Configuration configuration);

  void register(Map<String, String> attributes);

  String createRedirectToLoginUrl(
      String requestUrl,
      boolean duplicateRedirect,
      HttpServletRequest req,
      HttpServletResponse res
  );

  String getLoginPageUrl();

  String getLogoutUrl();

  SSOPrincipal validateUserToken(String authToken);

  boolean invalidateUserToken(String authToken);

  SSOPrincipal validateAppToken(String authToken, String componentId);

  boolean invalidateAppToken(String authToken);

  void clearCaches();

  void setRegistrationResponseDelegate(RegistrationResponseDelegate delegate);

}
