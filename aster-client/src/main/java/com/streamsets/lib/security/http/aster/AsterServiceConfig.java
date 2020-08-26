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

import com.streamsets.datacollector.util.Configuration;

/**
 * Aster SSO Service configuration.
 */
public class AsterServiceConfig {
  public static final String ASTER_URL = "aster.url";
  public static final String ASTER_URL_DEFAULT = "https://streamsets.dev:18632"; //TODO: SDC-15627

  private static final String ASTER_OAUTH_AUTHORIZE_PATH = "/login/oauth/access_token/elogin";
  private static final String ASTER_OAUTH_TOKEN_PATH = "/api/security/oauth/token";

  public static final String LOCAL_STATE_EXPIRATION_SECS = "aster.userLoginState.expiration.secs";
  public static final int LOCAL_STATE_EXPIRATION_SECS_DEFAULT = 5 * 60;
  public static final String ENGINE_ACCESS_TOKEN_MAX_EXPIRATION_SECS = "aster.engineAccessToken.expiration.secs";
  public static final int ENGINE_ACCESS_TOKEN_MAX_EXPIRATION_SECS_DEFAULT = 24 * 60 * 60;

  private static final String REGISTRATION_PATH = "/tregistration.html";
  private static final String REGISTRATION_REST_PATH = "/rest/v1/tregistration";
  private static final String REGISTRATION_CALLBACK_PATH = "/tregistration-callback.html";
  private static final String USER_LOGIN_PATH = "/tlogin.html";
  private static final String USER_LOGIN_REST_PATH = "/rest/v1/tlogin";
  private static final String USER_LOGIN_CALLBACK_PATH = "/tlogin-callback.html";

  private final AsterRestConfig clientConfig;
  private final Configuration engineConfig;

  /**
   * Constructor.
   * @param engineType the engine type, {@code DC} or {@code TF}.
   * @param engineVersion the engine version (from {@code BuildInfo}).
   * @param engineId the engine ID (from {@code RuntimeInfo}).
   * @param engineConfig the engine {@code Configuration}.
   */
  public AsterServiceConfig(
      AsterRestConfig.SubjectType engineType,
      String engineVersion,
      String engineId,
      Configuration engineConfig
  ) {
    this.engineConfig = engineConfig;

    String asterUrl = engineConfig.get(ASTER_URL, ASTER_URL_DEFAULT);

    int stateExp = engineConfig.get(LOCAL_STATE_EXPIRATION_SECS, LOCAL_STATE_EXPIRATION_SECS_DEFAULT);
    int accessTokenMaxExp = engineConfig.get(
        ENGINE_ACCESS_TOKEN_MAX_EXPIRATION_SECS,
        ENGINE_ACCESS_TOKEN_MAX_EXPIRATION_SECS_DEFAULT
    );

    clientConfig = new AsterRestConfig().setClientId(engineId)
        .setSubjectType(engineType)
        .setClientVersion(engineVersion)
        .setAuthorizeUri(asterUrl + ASTER_OAUTH_AUTHORIZE_PATH)
        .setTokenUri(asterUrl + ASTER_OAUTH_TOKEN_PATH)
        .setStateCacheExpirationSecs(stateExp)
        .setAccessTokenMaxExpInSecs(accessTokenMaxExp)
        .setRegistrationCallbackPath(REGISTRATION_CALLBACK_PATH)
        .setLoginCallbackPath(USER_LOGIN_CALLBACK_PATH);
  }

  /**
   * Returns the engine configuration.
   */
  public Configuration getEngineConfig() {
    return engineConfig;
  }

  /**
   * Returns the Aster REST client configuration.
   */
  public AsterRestConfig getAsterRestConfig() {
    return clientConfig;
  }

  /**
   * Returns the engine registration page URL.
   */
  public String getEngineRegistrationPath() {
    return REGISTRATION_PATH;
  }

  /**
   * Returns the engine registration REST endpoint URL.
   */
  public String getRegistrationUrlRestPath() {
    return REGISTRATION_REST_PATH;
  }

  /**
   * Returns the engine login page URL.
   */
  public String getUserLoginPath() {
    return USER_LOGIN_PATH;
  }

  /**
   * Returns the engine login REST endpoint URL.
   */
  public String getUserLoginRestPath() {
    return USER_LOGIN_REST_PATH;
  }

}
