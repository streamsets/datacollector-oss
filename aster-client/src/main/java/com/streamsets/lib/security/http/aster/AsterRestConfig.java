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

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Aster REST client configuration.
 * <p/>
 * It contains information to configure the Aster REST client such as timeous and engine callback URIs required for the
 * OAuth2 grants.
 */
public class AsterRestConfig {
  public static final int STATE_CACHE_EXPIRATION_SECS = (int) TimeUnit.MINUTES.toSeconds(5);
  public static final int MAX_ACCESS_TOKEN_EXPIRATION_SECS = (int) TimeUnit.HOURS.toSeconds(24);

  /**
   * Subject type to use in the Aster Oauth2 authorization requests.
   */
  public enum SubjectType {
    USER, DC, TF
  }

  private String clientId;
  private SubjectType subjectType;
  private String clientVersion;
  private String registrationCallbackPath;
  private String loginCallbackPath;

  private String asterUrl;

  private int stateCacheExpirationSecs = STATE_CACHE_EXPIRATION_SECS;

  private int accessTokenMaxExpInSecs = MAX_ACCESS_TOKEN_EXPIRATION_SECS;


  /**
   * Convenience method that asserts all the configuration properties are set.
   */
  public AsterRestConfig assertValid() {
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(subjectType, "clientType");
    Preconditions.checkNotNull(clientVersion, "clientVersion");
    Preconditions.checkNotNull(registrationCallbackPath, "clientCallbackPath");
    Preconditions.checkNotNull(loginCallbackPath, "loginCallbackPath");
    Preconditions.checkNotNull(asterUrl, "asterUrl");
    Preconditions.checkState(stateCacheExpirationSecs > 0, "stateCacheExpirationSecs must be greater than zero");
    Preconditions.checkState(accessTokenMaxExpInSecs > 0, "accessTokenMaxExpInSecs must be greater than zero");
    return this;
  }

  /**
   * Returns the clientId to use with Aster, this is the engine ID (i.e. the SDC ID).
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the clientId to use with Aster, this is the engine ID (i.e. the SDC ID).
   */
  public AsterRestConfig setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  /**
   * Returns the subject type to use in the Aster Oauth2 authorization requests.
   */
  public SubjectType getSubjectType() {
    return subjectType;
  }

  /**
   * Sets the subject type to use in the Aster Oauth2 authorization requests.
   */
  public AsterRestConfig setSubjectType(SubjectType subjectType) {
    this.subjectType = subjectType;
    return this;
  }

  /**
   * Returns the engine version.
   */
  public String getClientVersion() {
    return clientVersion;
  }

  /**
   * Sets the engine version.
   */
  public AsterRestConfig setClientVersion(String clientVersion) {
    this.clientVersion = clientVersion;
    return this;
  }

  /**
   * Returns the engine registration callback URI path to use during an engine registration request.
   */
  public String getRegistrationCallbackPath() {
    return registrationCallbackPath;
  }

  /**
   * Sets the engine registration callback URI path to use during an engine registration request.
   */
  public AsterRestConfig setRegistrationCallbackPath(String registrationCallbackPath) {
    this.registrationCallbackPath = registrationCallbackPath;
    return this;
  }

  /**
   * Returns the engine login callback URI path to use during a user login request.
   */
  public String getLoginCallbackPath() {
    return loginCallbackPath;
  }

  /**
   * Sets the engine login callback URI path to use during a user login request.
   */
  public AsterRestConfig setLoginCallbackPath(String loginCallbackPath) {
    this.loginCallbackPath = loginCallbackPath;
    return this;
  }

  /**
   * Returns the Aster base URL to construct endpoints.
   */
  public String getAsterUrl() {
    return asterUrl;
  }

  /**
   * Sets the Aster base URL to construct endpoints.
   */
  public AsterRestConfig setAsterUrl(String asterUrl) {
    this.asterUrl = asterUrl;
    return this;
  }

  /**
   * Returns the OAuth2 request state cache expiration interval.
   */
  public int getStateCacheExpirationSecs() {
    return stateCacheExpirationSecs;
  }

  /**
   * Sets the OAuth2 request state cache expiration interval.
   */
  public AsterRestConfig setStateCacheExpirationSecs(int stateCacheExpirationSecs) {
    this.stateCacheExpirationSecs = stateCacheExpirationSecs;
    return this;
  }

  /**
   * Returns the maximum (engine local) expiration interval for access tokens regardless of the received expiration
   * interval received in the token grant.
   */
  public int getAccessTokenMaxExpInSecs() {
    return accessTokenMaxExpInSecs;
  }

  /**
   * Sets the maximum (engine local) expiration interval for access tokens regardless of the received expiration
   * interval received in the token grant.
   */
  public AsterRestConfig setAccessTokenMaxExpInSecs(int accessTokenMaxExpInSecs) {
    this.accessTokenMaxExpInSecs = accessTokenMaxExpInSecs;
    return this;
  }

}
