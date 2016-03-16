/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

public class RemoteSSOService implements SSOService {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteSSOService.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String CONFIG_PREFIX = SSOUserAuthenticator.CONFIG_PREFIX + "service.";

  public static final String SECURITY_SERVICE_BASE_URL_CONFIG = CONFIG_PREFIX + "url";
  public static final String SECURITY_SERVICE_BASE_URL_DEFAULT = "http://localhost:18631/security";

  public static final String SECURITY_SERVICE_AUTH_TOKEN_CONFIG = CONFIG_PREFIX + "authToken";

  public static final String SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG =
      CONFIG_PREFIX + "validateAuthToken.secs";

  public static final long SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT = 10 * 60;

  public static final int INITIAL_FETCH_INFO_FREQUENCY = 10 * 60;

  private String loginPageUrl;
  private String forServicesUrl;
  private String appAuthUrl;
  private volatile SSOTokenParser tokenParser;
  private Listener listener;
  private volatile long securityInfoFetchFrequency;
  private volatile long lastSecurityInfoFetchTime;
  private String ownAuthToken;
  private long validateAppTokenFrequency;

  @Override
  public void setDelegateTo(SSOService ssoService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfiguration(Configuration conf) {
    String baseUrl = conf.get(SECURITY_SERVICE_BASE_URL_CONFIG, SECURITY_SERVICE_BASE_URL_DEFAULT);
    Utils.checkArgument(
        baseUrl.toLowerCase().startsWith("http:") || baseUrl.toLowerCase().startsWith("https:"),
        Utils.formatL("Security service base URL must be HTTP/HTTPS '{}'", baseUrl)
    );
    if (baseUrl.toLowerCase().startsWith("http://")) {
      LOG.warn("Security service base URL is not secure '{}'", baseUrl);
    }
    loginPageUrl = baseUrl + "/login";
    forServicesUrl = baseUrl + "/public-rest/v1/for-client-services";
    appAuthUrl = baseUrl + "/rest/v1/componentAuth";
    ownAuthToken = conf.get(SECURITY_SERVICE_AUTH_TOKEN_CONFIG, null);
    if (ownAuthToken == null) {
      LOG.info("The '{}' property is not set, apps authentication is disabled", SECURITY_SERVICE_AUTH_TOKEN_CONFIG);
    }
    validateAppTokenFrequency =
        conf.get(SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_CONFIG, SECURITY_SERVICE_VALIDATE_AUTH_TOKEN_FREQ_DEFAULT);

    securityInfoFetchFrequency = INITIAL_FETCH_INFO_FREQUENCY;
    fetchInfoForClientServices();
  }

  String getLoginPageUrl() {
    return loginPageUrl;
  }

  String getForServicesUrl() {
    return forServicesUrl;
  }

  long getSecurityInfoFetchFrequency() {
    return securityInfoFetchFrequency;
  }

  boolean hasAuthToken() {
    return ownAuthToken != null;
  }

  @Override
  public String createRedirectToLoginURL(String requestUrl) {
    try {
      return loginPageUrl + "?" + SSOConstants.REQUESTED_URL_PARAM + "=" + URLEncoder.encode(requestUrl, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(Utils.format("Should not happen: {}", ex.toString()), ex);
    }
  }

  @Override
  public SSOTokenParser getTokenParser() {
    return tokenParser;
  }

  @Override
  public void setListener(Listener listener) {
    this.listener = listener;
  }

  boolean isTimeToRefresh() {
    return System.currentTimeMillis() - lastSecurityInfoFetchTime > getSecurityInfoFetchFrequency() * 1000;
  }

  HttpURLConnection getSecurityInfoConnection() throws IOException {
    URL url = new URL(forServicesUrl);
    return (HttpURLConnection) url.openConnection();
  }


  @VisibleForTesting
  @SuppressWarnings("unchecked")
  void fetchInfoForClientServices() {
    LOG.debug("Fetching info for client services");
    try {
      HttpURLConnection conn = getSecurityInfoConnection();
      conn.setUseCaches(false);
      conn.setConnectTimeout(1000);
      conn.setReadTimeout(1000);
      conn.setRequestProperty(SSOConstants.X_REST_CALL, "-");
      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        Map map = OBJECT_MAPPER.readValue(conn.getInputStream(), Map.class);
        String tokenVerification = (String) map.get(SSOConstants.TOKEN_VERIFICATION_TYPE);
        if (tokenVerification != null) {
          switch (tokenVerification) {
            case PlainSSOTokenParser.TYPE:
              if (tokenParser == null || !tokenParser.getType().equals(PlainSSOTokenParser.TYPE)) {
                LOG.debug("Got token verfication type '{}'", tokenVerification);
                tokenParser = new PlainSSOTokenParser();
              }
              break;
            case SignedSSOTokenParser.TYPE:
              if (tokenParser == null || !tokenParser.getType().equals(SignedSSOTokenParser.TYPE)) {
                LOG.debug("Got token verfication type '{}'", tokenVerification);
                tokenParser = new SignedSSOTokenParser();
              }
              break;
            default:
              LOG.error("Invalidate token verification '{}'", tokenVerification);
              tokenParser = null;
          }

        }
        String publicKey = (String) map.get(SSOConstants.TOKEN_VERIFICATION_DATA);
        if (publicKey != null) {
          LOG.debug("Got new token verfication data, refreshing token parser");
          if (getTokenParser() != null) {
            getTokenParser().setVerificationData(publicKey);
          } else {
            LOG.error("Got token verification data but there is no parser available");
          }
        }
        List<String> invalidateTokenIds = (List<String>) map.get(SSOConstants.INVALIDATE_TOKEN_IDS);
        if (invalidateTokenIds != null) {
          LOG.debug("Got '{}' tokens to invalidate", invalidateTokenIds.size());
          if (listener != null) {
            listener.invalidate(invalidateTokenIds);
          } else {
            LOG.warn("No listener set to invalidate tokens");
          }
        }
        if (map.containsKey(SSOConstants.FETCH_INFO_FREQUENCY)) {
          long fetchFrequency = (Integer) map.get(SSOConstants.FETCH_INFO_FREQUENCY);
          if (securityInfoFetchFrequency != fetchFrequency) {
            LOG.debug("Fetch frequency changed to '{}' secs", fetchFrequency);
            securityInfoFetchFrequency = fetchFrequency;
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Could not get info Security service: {}", ex.toString(), ex);
    }
  }

  @Override
  public void refresh() {
    if (isTimeToRefresh()) {
      boolean fetch;
      synchronized (this) {
        fetch = isTimeToRefresh();
        lastSecurityInfoFetchTime = System.currentTimeMillis();
      }
      if (fetch) {
        fetchInfoForClientServices();
      }
    }
  }

  HttpURLConnection getAuthTokeValidationConnection() throws IOException {
    URL url = new URL(appAuthUrl);
    return (HttpURLConnection) url.openConnection();
  }

  @Override
  public boolean isAppAuthenticationEnabled() {
    return ownAuthToken != null;
  }

  @Override
  public SSOUserPrincipal validateAppToken(String authToken, String componentId) {
    SSOUserPrincipalJson principal;
    Utils.checkState(hasAuthToken(), "App token validation is disabled");
    try {
      HttpURLConnection conn = getAuthTokeValidationConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setUseCaches(false);
      conn.setConnectTimeout(1000);
      conn.setReadTimeout(1000);
      conn.setRequestProperty(SSOConstants.X_REST_CALL, "-");
      conn.setRequestProperty(SSOConstants.X_APP_AUTH_TOKEN, ownAuthToken);
      ComponentAuthJson authTokenJson = new ComponentAuthJson();
      authTokenJson.setComponentId(componentId);
      authTokenJson.setAuthToken(authToken);
      OutputStream os = conn.getOutputStream();
      OBJECT_MAPPER.writeValue(os, authTokenJson);
      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        principal = OBJECT_MAPPER.readValue(conn.getInputStream(), SSOUserPrincipalJson.class);
        principal.setTokenStr(authToken);
        principal.lock();
        LOG.debug(
            "Validated app auth token for '{}' from '{}' organization",
            principal.getPrincipalId(),
            principal.getOrganizationId()
        );
      } else {
        LOG.warn(
            "Security service HTTP error '{}': {}",
            conn.getResponseCode(),
            conn.getResponseMessage()
        );
        principal = null;
      }
    } catch (IOException ex) {
      LOG.warn("Failed to validate app auth token: {}", ex.toString(), ex);
      principal = null;
    }
    return principal;
  }

  @Override
  public long getValidateAppTokenFrequency() {
    return validateAppTokenFrequency;
  }

}
