/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.lib.security.http;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSSOService implements SSOService {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSSOService.class);

  public static final String CONFIG_PREFIX = "dpm.";

  public static final String SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG =
      CONFIG_PREFIX + "security.validationTokenFrequency.secs";

  public static final long SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT = 60;

  private String loginPageUrl;
  private String logoutUrl;
  private PrincipalCache userPrincipalCache;
  private PrincipalCache appPrincipalCache;

  @Override
  public void setDelegateTo(SSOService ssoService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfiguration(Configuration conf) {
    long validateAuthTokenFrequencySecs =
        conf.get(SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT);
    initializePrincipalCaches(TimeUnit.SECONDS.toMillis(validateAuthTokenFrequencySecs));
  }

  protected void setLoginPageUrl(String loginPageUrl) {
    this.loginPageUrl = loginPageUrl;
  }

  protected void setLogoutUrl(String logoutUrl) {
    this.logoutUrl = logoutUrl;
  }

  void initializePrincipalCaches(long ttlMillis) {
    userPrincipalCache = new PrincipalCache(ttlMillis);
    appPrincipalCache = new PrincipalCache(ttlMillis);
  }

  protected PrincipalCache getUserPrincipalCache() {
    return userPrincipalCache;
  }

  protected PrincipalCache getAppPrincipalCache() {
    return appPrincipalCache;
  }

  @Override
  public String createRedirectToLoginUrl(String requestUrl, boolean repeatedRedirect) {
    try {
      String url = loginPageUrl + "?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode(requestUrl, "UTF-8");
      if (repeatedRedirect) {
        url = url + "&" + SSOConstants.REPEATED_REDIRECT_PARAM + "=";
      }
      return url;
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(Utils.format("Should not happen: {}", ex.toString()), ex);
    }
  }

  String getLoginPageUrl() {
    return loginPageUrl;
  }

  @Override
  public String getLogoutUrl() {
    return logoutUrl;
  }

  @Override
  public SSOPrincipal validateUserToken(String authToken) {
    SSOPrincipal principal = userPrincipalCache.get(authToken);
    if (principal == null) {
      if (userPrincipalCache.isInvalid(authToken)) {
        LOG.debug("User token (cached) invalid '{}'", authToken);
      } else {
        principal = validateUserTokenWithSecurityService(authToken);
        if (principal != null) {
          userPrincipalCache.put(authToken, principal);
        } else {
          LOG.debug("User token invalid '{}'", authToken);
          userPrincipalCache.invalidate(authToken);
        }
      }
    }
    return principal;
  }

  @Override
  public boolean invalidateUserToken(String authToken) {
    return userPrincipalCache.invalidate(authToken);
  }

  protected abstract SSOPrincipal validateUserTokenWithSecurityService(String authToken);

  @Override
  public SSOPrincipal validateAppToken(String authToken, String componentId) {
    SSOPrincipal principal = appPrincipalCache.get(authToken);
    if (principal == null) {
      if (appPrincipalCache.isInvalid(authToken)) {
        LOG.debug("App token invalid '{}'", authToken);
      } else {
        principal = validateAppTokenWithSecurityService(authToken, componentId);
        if (principal != null) {
          appPrincipalCache.put(authToken, principal);
        } else {
          LOG.debug("App token invalid '{}'", authToken);
          appPrincipalCache.invalidate(authToken);
        }
      }
    } else {
      if (!principal.getPrincipalId().equals(componentId)) {
        principal = null;
      }
    }
    return principal;
  }

  @Override
  public boolean invalidateAppToken(String authToken) {
    return appPrincipalCache.invalidate(authToken);
  }

  protected abstract SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId);

}
