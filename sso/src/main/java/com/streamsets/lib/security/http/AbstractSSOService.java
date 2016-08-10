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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    return validate(userPrincipalCache, createUserRemoteValidation(authToken), authToken, "User");
  }

  @Override
  public boolean invalidateUserToken(String authToken) {
    return userPrincipalCache.invalidate(authToken);
  }

  protected abstract SSOPrincipal validateUserTokenWithSecurityService(String authToken);

  @Override
  public SSOPrincipal validateAppToken(String authToken, String componentId) {
    SSOPrincipal principal =
        validate(appPrincipalCache, createAppRemoteValidation(authToken, componentId), authToken, "App");
    if (principal != null && !principal.getPrincipalId().equals(componentId)) {
      principal = null;
    }
    return principal;
  }

  @Override
  public boolean invalidateAppToken(String authToken) {
    return appPrincipalCache.invalidate(authToken);
  }

  protected abstract SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId);

  private static final Object DUMMY = new Object();
  private ConcurrentMap<String, Object> lockMap = new ConcurrentHashMap<>();

  @VisibleForTesting
  ConcurrentMap<String, Object> getLockMap() {
    return lockMap;
  }

  SSOPrincipal validate(PrincipalCache cache, Callable<SSOPrincipal> remoteValidation, String token, String type) {
    SSOPrincipal principal = cache.get(token);
    if (principal == null) {
      if (cache.isInvalid(token)) {
        LOG.debug("{} token (cached) invalid '{}'", type, token);
      } else {
        long start = System.currentTimeMillis();
        while (getLockMap().putIfAbsent(token, DUMMY) != null) {
          if (System.currentTimeMillis() - start > 10000) {
            String msg = "Exceeded 10sec max wait time";
            LOG.warn(msg);
            throw new RuntimeException(msg);
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException ex) {
            LOG.warn("Got interrupted while waiting for lock");
            return null;
          }
        }
        try {
          principal = cache.get(token);
          if (principal == null) {
            try {
              principal = remoteValidation.call();
            } catch (Exception ex) {
              if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
              } else {
                throw new RuntimeException(ex);
              }
            }
            if (principal != null) {
              cache.put(token, principal);
            } else {
              LOG.debug("{} token invalid '{}'", type, token);
              cache.invalidate(token);
            }
          }
        } finally {
          getLockMap().remove(token);
        }
      }
    }
    return principal;
  }

  Callable<SSOPrincipal> createUserRemoteValidation(final String authToken) {
    return new Callable<SSOPrincipal>() {
      @Override
      public SSOPrincipal call() throws Exception {
        return validateUserTokenWithSecurityService(authToken);
      }
    };
  }

  Callable<SSOPrincipal> createAppRemoteValidation(final String authToken, final String componentId) {
    return new Callable<SSOPrincipal>() {
      @Override
      public SSOPrincipal call() throws Exception {
        return validateAppTokenWithSecurityService(authToken, componentId);
      }
    };
  }

}
