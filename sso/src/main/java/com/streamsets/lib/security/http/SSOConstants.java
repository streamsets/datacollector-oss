/*
 * Copyright 2018 StreamSets Inc.
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

/**
 * SSO public constants.
 */
public interface  SSOConstants {
  /**
   * Authentication method name for StreamSets single sign on.
   */
  String AUTHENTICATION_METHOD = "SS-SSO";

  String AUTHENTICATION_COOKIE_PREFIX = "SS-SSO-";

  /**
   * Name for cookie that stores requested URL when login is required.
   */
  String REQUESTED_URL_COOKIE = AUTHENTICATION_COOKIE_PREFIX + "REQUESTED-URL";

  /**
   * Header to indicate the requester is making a REST call (as opposed to a page view).
   */
  String X_REST_CALL = "X-Requested-By";

  /**
   * Header with the user authentication token. This header is returned to the client by the {@link SSOUserAuthenticator}
   * and
   * the client should include it in all REST calls to StreamSets services.
   */
  String X_USER_AUTH_TOKEN = "X-SS-User-Auth-Token";

  /**
   * Header with the app authentication token. This header is returned to the client by the {@link SSOAppAuthenticator}
   * and
   * the client should include it in all REST calls to StreamSets services.
   */
  String X_APP_AUTH_TOKEN = "X-SS-App-Auth-Token";

  /**
   * Header with the app authentication token. This header is returned to the client by the {@link SSOAppAuthenticator}
   * and
   * the client should include it in all REST calls to StreamSets services.
   */
  String X_APP_COMPONENT_ID = "X-SS-App-Component-Id";

  /**
   * Header to be used for connection and read timeouts. The timeout value in millis is used by clients making REST
   * calls to StreamSets services.
   */
  String X_APP_CONNECTION_TIMEOUT = "X-SS-CONNECTION-TIMEOUT";

  /**
   * Query string parameter with the user authentication token. The parameter contains the authentication token for
   * the user. This parameter is returned by the StreamSets security service with a redirection to the original page
   * that triggered the redirection to authentication.
   */
  String USER_AUTH_TOKEN_PARAM = "ss-userAuthToken";

  /**
   * Query string parameter with the original request URL. This paramater is used when redirecting a user to the
   * authentication service. After authentication, the user will be redirected back to this URL.
   */
  String REQUESTED_URL_PARAM = "ss-requestedUrl";

  String REPEATED_REDIRECT_PARAM = "ss-repeated-redirect";

  String SERVICE_BASE_URL_ATTR = "baseHttpUrl";

  String SDC_COMPONENT_NAME = "SDC";
}
