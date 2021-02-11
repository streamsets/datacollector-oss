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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
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

  public static final String SEND_SYNC_EVENTS = "dpm.should.send.sync.events";

  public static final long SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT = 60;

  public static final String DOMAIN_FOR_REQUESTED_URL_COOKIE = CONFIG_PREFIX + "domainForRequestedUrlCookie";

  public static final String USE_QUERY_STRING_FOR_REQUESTED_URL = CONFIG_PREFIX + "useQueryStringForRequestedUrl";
  public static final boolean USE_QUERY_STRING_FOR_REQUESTED_URL_DEFAULT = true;

  private String loginPageUrl;
  private String logoutUrl;
  private PrincipalCache userPrincipalCache;
  private PrincipalCache appPrincipalCache;
  private long connectionTimeout;
  private boolean useQueryStringForRequestedUrl;
  private String domainForRequestedUrlCookie;

  protected RegistrationResponseDelegate registrationResponseDelegate;

  @Override
  public void setDelegateTo(SSOService ssoService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SSOService getDelegateTo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfiguration(Configuration conf) {
    long validateAuthTokenFrequencySecs =
        conf.get(SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT);
    Utils.checkArgument(validateAuthTokenFrequencySecs >= 30, Utils.format(
        "Configuration '{}' set to '{}' seconds, it must be at least '{}' secs",
        SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG,
        validateAuthTokenFrequencySecs,
        30
    ));
    connectionTimeout = conf.get(RemoteSSOService.SECURITY_SERVICE_CONNECTION_TIMEOUT_CONFIG,
        RemoteSSOService.DEFAULT_SECURITY_SERVICE_CONNECTION_TIMEOUT);
    useQueryStringForRequestedUrl = conf.get(
        USE_QUERY_STRING_FOR_REQUESTED_URL,
        USE_QUERY_STRING_FOR_REQUESTED_URL_DEFAULT);
    domainForRequestedUrlCookie = conf.get(DOMAIN_FOR_REQUESTED_URL_COOKIE, null);

    initializePrincipalCaches(TimeUnit.SECONDS.toMillis(validateAuthTokenFrequencySecs));
  }

  protected void setLoginPageUrl(String loginPageUrl) {
    this.loginPageUrl = loginPageUrl;
  }

  protected void setLogoutUrl(String logoutUrl) {
    this.logoutUrl = logoutUrl;
  }

  void initializePrincipalCaches(long ttlMillis) {
    //for user tokens, once a token is invalid that it, so we should cache the invalid one for a while to avoid the
    // full check
    userPrincipalCache = new PrincipalCache(ttlMillis, TimeUnit.HOURS.toMillis(1));
    // for app tokens, an app token can be invalid because of being deactivated but once reactivated it will be
    // valid again, so the caching time should be the same as for valid app tokens
    appPrincipalCache = new PrincipalCache(ttlMillis, ttlMillis);
  }

  protected PrincipalCache getUserPrincipalCache() {
    return userPrincipalCache;
  }

  protected PrincipalCache getAppPrincipalCache() {
    return appPrincipalCache;
  }

  @VisibleForTesting
  String getRequestedFullPath(String requestUrl) {
    try {
      URL url = new URL(requestUrl);
      String requestFullPath = url.getPath();
      if (url.getQuery() != null) {
        requestFullPath += "?" + url.getQuery();
      }
      if (url.getRef() != null) {
        requestFullPath += "#" + url.getRef();
      }
      return requestFullPath.isEmpty() ? "/" : requestFullPath;
    } catch (MalformedURLException ex) {
      throw new RuntimeException(Utils.format("Should not happen: {}", ex.toString()), ex);
    }
  }

  @VisibleForTesting
  String createRedirectToLoginUrlInCookie(
      String requestUrl,
      boolean repeatedRedirect,
      HttpServletRequest req,
      HttpServletResponse res
  ) {
    try {
      requestUrl = getRequestedFullPath(requestUrl);
      Cookie cookie = new Cookie(SSOConstants.REQUESTED_URL_COOKIE, URLEncoder.encode(requestUrl, "UTF-8"));
      cookie.setMaxAge(60);
      cookie.setPath("/");
      if (domainForRequestedUrlCookie != null) {
        cookie.setDomain(domainForRequestedUrlCookie);
      }
      res.addCookie(cookie);

      String loginUrl = getLoginPageUrl();
      if (repeatedRedirect) {
        loginUrl += "?" + SSOConstants.REPEATED_REDIRECT_PARAM + "=";
      }
      return loginUrl;
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(Utils.format("Should not happen: {}", ex.toString()), ex);
    }
  }

  @VisibleForTesting
  String createRedirectToLoginUrlInQueryString(
      String requestUrl,
      boolean repeatedRedirect,
      HttpServletRequest req,
      HttpServletResponse res
  ) {
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

  @Override
  public String createRedirectToLoginUrl(
      String requestUrl,
      boolean duplicateRedirect,
      HttpServletRequest req,
      HttpServletResponse res
  ) {
    return (useQueryStringForRequestedUrl)
        ? createRedirectToLoginUrlInQueryString(requestUrl, duplicateRedirect, req, res)
        : createRedirectToLoginUrlInCookie(requestUrl, duplicateRedirect, req, res);
  }

  @Override
  public String getLoginPageUrl() {
    return loginPageUrl;
  }

  @Override
  public String getLogoutUrl() {
    return logoutUrl;
  }

  @Override
  public SSOPrincipal validateUserToken(String authToken) {
    return validate(userPrincipalCache, createUserRemoteValidation(authToken), authToken, "-", "User");
  }

  @Override
  public boolean invalidateUserToken(String authToken) {
    return userPrincipalCache.invalidate(authToken);
  }

  // returns principal if OK, throws ForbiddenException if invalid credentials (will add to invalid cache) or
  // throws RuntimeException (if connection issues, won't add to invalid cache
  protected abstract SSOPrincipal validateUserTokenWithSecurityService(String authToken) throws ForbiddenException;

  @Override
  public SSOPrincipal validateAppToken(String authToken, String componentId) {
    SSOPrincipal principal =
        validate(appPrincipalCache, createAppRemoteValidation(authToken, componentId), authToken, componentId, "App");
    if (principal != null && !principal.getPrincipalId().equals(componentId)) {
      principal = null;
    }
    return principal;
  }

  @Override
  public boolean invalidateAppToken(String authToken) {
    return appPrincipalCache.invalidate(authToken);
  }

  @Override
  public void clearCaches() {
    getUserPrincipalCache().clear();
    getAppPrincipalCache().clear();
    LOG.info("Flushed user and application principal caches");
  }

  @Override
  public void setRegistrationResponseDelegate(RegistrationResponseDelegate delegate) {
    this.registrationResponseDelegate = delegate;
  }

  // returns principal if OK, throws ForbiddenException if invalid credentials (will add to invalid cache) or
  // throws RuntimeException (if connection issues, won't add to invalid cache
  protected abstract SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId)
      throws ForbiddenException;

  private static final Object DUMMY = new Object();
  private ConcurrentMap<String, Object> lockMap = new ConcurrentHashMap<>();

  @VisibleForTesting
  ConcurrentMap<String, Object> getLockMap() {
    return lockMap;
  }

  private void trace(String message, String token, String... params) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(message, SSOUtils.tokenForLog(token), params);
    }
  }

  SSOPrincipal validate(PrincipalCache cache, Callable<SSOPrincipal> remoteValidation, String token, String
      componentId, String type) {
    SSOPrincipal principal = cache.get(token);
    String tokenForLog = SSOUtils.tokenForLog(token);
    if (principal == null) {
      if (cache.isInvalid(token)) {
        LOG.debug("Token '{}' invalid '{}' for component '{}'", type, tokenForLog, componentId);
      } else {
        trace("Trying to get lock for token '{}' component '{}'", tokenForLog, componentId);
        long start = System.currentTimeMillis();
        int counter = 0;
        while (getLockMap().putIfAbsent(token, DUMMY) != null) {
          if (++counter % 1000 == 0) {
            trace("Retrying getting lock for token '{}' component '{}'", tokenForLog, componentId);
          }
          counter++;
          if (System.currentTimeMillis() - start > (connectionTimeout + 1000)) {
            String msg = Utils.format("Exceeded '{}' millis max wait time trying to validate component '{}'",
                connectionTimeout,
                componentId
            );
            LOG.warn(msg);
            throw new RuntimeException(msg);
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException ex) {
            LOG.warn(
                "Got interrupted while waiting for lock for token '{}' for component '{}'",
                tokenForLog,
                componentId
            );
            return null;
          }
        }
        trace("Got lock for token '{}' component '{}'", token, componentId);
        try {
          principal = cache.get(token);
          if (principal == null) {
            LOG.debug("Token '{}' component '{}' not found in cache", tokenForLog, componentId);
            try {
              principal = remoteValidation.call();
              trace("Adding token '{}' for component '{}' to cache", tokenForLog, componentId);
              cache.put(token, principal);
            } catch (ForbiddenException fex) {
              cache.invalidate(token);
              trace("ForbiddenToken '{}' invalid '{}', invalidating in cache", tokenForLog, componentId);
              throw fex;
            } catch (MovedException mex) {
              throw mex;
            } catch (Exception ex) {
              if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
              } else {
                throw new RuntimeException(ex);
              }
            }
          } else {
            LOG.debug("Token '{}' component '{}' found in cache", tokenForLog, componentId);
          }
        } catch (MovedException mex) {
          throw mex;
        } catch (Exception ex) {
          LOG.error(
              "Exception while doing remote validation for token '{}' component '{}': {}",
              tokenForLog,
              componentId,
              ex.toString()
          );
        } finally {
          trace("Released lock for token '{}' component '{}'", tokenForLog, componentId);
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
