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
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RemoteSSOService extends AbstractSSOService {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteSSOService.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DPM_BASE_URL_CONFIG = "dpm.base.url";
  public static final String DPM_BASE_URL_DEFAULT = "http://localhost:18631";

  public static final String SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG = CONFIG_PREFIX + "appAuthToken";
  public static final String SECURITY_SERVICE_COMPONENT_ID_CONFIG = CONFIG_PREFIX + "componentId";

  public static final String CONTENT_TYPE = "Content-Type";
  public static final String ACCEPT = "Accept";
  public static final String APPLICATION_JSON = "application/json";

  private String userAuthUrl;
  private String appAuthUrl;
  private String appToken;
  private String componentId;

  @Override
  public void setDelegateTo(SSOService ssoService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfiguration(Configuration conf) {
    super.setConfiguration(conf);
    String dpmBaseUrl = getValidURL(conf.get(DPM_BASE_URL_CONFIG, DPM_BASE_URL_DEFAULT));
    String baseUrl = dpmBaseUrl + "security";

    Utils.checkArgument(
        baseUrl.toLowerCase().startsWith("http:") || baseUrl.toLowerCase().startsWith("https:"),
        Utils.formatL("Security service base URL must be HTTP/HTTPS '{}'", baseUrl)
    );
    if (baseUrl.toLowerCase().startsWith("http://")) {
      LOG.warn("Security service base URL is not secure '{}'", baseUrl);
    }
    setLoginPageUrl(baseUrl + "/login");
    setLogoutUrl(baseUrl + "/_logout");
    userAuthUrl = baseUrl + "/rest/v1/validateAuthToken/user";
    appAuthUrl = baseUrl + "/rest/v1/validateAuthToken/component";
    appToken = conf.get(SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, null);
  }

  public void setComponentId(String componentId) {
    Utils.checkArgument(componentId != null && !componentId.trim().isEmpty(), "Component ID cannot be NULL or empty");
    this.componentId = componentId.trim();
  }

  public void setApplicationAuthToken(String appToken) {
    this.appToken = (appToken != null) ? appToken.trim() : null;
  }

  HttpURLConnection createAuthConnection(String url) throws IOException {
    return (HttpURLConnection) new URL(url).openConnection();
  }

  HttpURLConnection getAuthConnection(String method, String url) throws IOException {
    HttpURLConnection conn = createAuthConnection(url);
    conn.setRequestMethod(method);
    switch (method) {
      case "POST":
      case "PUT":
        conn.setDoOutput(true);
      case "GET":
        conn.setDoInput(true);
    }
    conn.setUseCaches(false);
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);
    conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
    conn.setRequestProperty(ACCEPT, APPLICATION_JSON);
    conn.setRequestProperty(SSOConstants.X_REST_CALL, "-");
    conn.setRequestProperty(SSOConstants.X_APP_AUTH_TOKEN, appToken);
    conn.setRequestProperty(SSOConstants.X_APP_COMPONENT_ID, componentId);
    return conn;
  }

  <T> T doAuthRestCall(String url, Object uploadData, Class<T> responseType) {
    T ret = null;
    String method = (uploadData == null) ? "GET" : "POST";
    try {
      HttpURLConnection conn = getAuthConnection(method, url);
      if (uploadData != null) {
        OutputStream os = conn.getOutputStream();
        OBJECT_MAPPER.writeValue(os, uploadData);
      }
      if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        ret = (T) OBJECT_MAPPER.readValue(conn.getInputStream(), responseType);
      } else {
        LOG.warn("Security service HTTP error '{}': {}", conn.getResponseCode(), conn.getResponseMessage());
      }
    } catch (IOException ex) {
      LOG.warn("Security service error: {}", ex.toString(), ex);
    }
    return ret;
  }


  protected SSOPrincipal validateUserTokenWithSecurityService(String authToken) {
    ValidateUserAuthTokenJson authTokenJson = new ValidateUserAuthTokenJson();
    authTokenJson.setAuthToken(authToken);
    SSOPrincipalJson principal = doAuthRestCall(userAuthUrl, authTokenJson, SSOPrincipalJson.class);
    if (principal != null) {
      principal.setTokenStr(authToken);
      principal.lock();
      LOG.debug("Validated user auth token for '{}'", principal.getPrincipalId());
    } else {
      LOG.warn("Failed to validate user token '{}'", authToken);
    }
    return principal;
  }

  protected SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId) {
    ValidateComponentAuthTokenJson authTokenJson = new ValidateComponentAuthTokenJson();
    authTokenJson.setComponentId(componentId);
    authTokenJson.setAuthToken(authToken);
    SSOPrincipalJson principal = doAuthRestCall(appAuthUrl, authTokenJson, SSOPrincipalJson.class);
    if (principal != null) {
      principal.setTokenStr(authToken);
      principal.lock();
      LOG.debug("Validated app auth token for '{}'", principal.getPrincipalId());
    } else {
      LOG.warn("Failed to validate app token '{}'", authToken);
    }
    return principal;
  }

  public static String getValidURL(String url) {
    if (!url.endsWith("/")) {
      url += "/";
    }
    return url;
  }

}
