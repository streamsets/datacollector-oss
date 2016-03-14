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

  public static final int INITIAL_FETCH_INFO_FREQUENCY = 10 * 60;

  private String loginPageUrl;
  private String forServicesUrl;
  private volatile SSOTokenParser tokenParser;
  private Listener listener;
  private volatile long securityInfoFetchFrequency;
  private volatile long lastSecurityInfoFetchTime;

  public RemoteSSOService(Configuration conf) {
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
    securityInfoFetchFrequency = INITIAL_FETCH_INFO_FREQUENCY;
  }

  @Override
  public void init() {
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


}
