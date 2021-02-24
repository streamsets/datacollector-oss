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
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.RegistrationResponseJson;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class RemoteSSOService extends AbstractSSOService {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteSSOService.class);

  public static final String DPM_BASE_URL_CONFIG = "dpm.base.url";
  public static final String DPM_APP_SECURITY_URL_CONFIG = "dpm.app.security.url";
  public static final String DPM_BASE_URL_DEFAULT = "http://localhost:18631";

  public static final String SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG = CONFIG_PREFIX + "appAuthToken";
  public static final String SECURITY_SERVICE_COMPONENT_ID_CONFIG = CONFIG_PREFIX + "componentId";
  public static final String SECURITY_SERVICE_CONNECTION_TIMEOUT_CONFIG = CONFIG_PREFIX + "connectionTimeout.millis";
  public static final String SECURITY_SERVICE_REMOTE_SSO_DISABLED_CONFIG = CONFIG_PREFIX + "remoteSso.disabled";
  public static final boolean SECURITY_SERVICE_REMOTE_SSO_DISABLED_DEFAULT = false;
  public static final String DPM_DEPLOYMENT_ID = "dpm.remote.deployment.id";
  public static final boolean DPM_USER_ALIAS_NAME_ENABLED_DEFAULT = false;
  public static final String DPM_USER_ALIAS_NAME_ENABLED = CONFIG_PREFIX + "alias.name.enabled";
  // Make the timeout consistent with SCH's default query timeout of 60 secs
  // TODO - Separate connect and read timeout configs
  public static final int DEFAULT_SECURITY_SERVICE_CONNECTION_TIMEOUT = 60000;
  public static final String DPM_ENABLED = CONFIG_PREFIX + "enabled";
  public static final boolean DPM_ENABLED_DEFAULT = false;
  public static final String DPM_REGISTRATION_RETRY_ATTEMPTS = "registration.retry.attempts";
  public static final int DPM_REGISTRATION_RETRY_ATTEMPTS_DEFAULT = 5;
  public static final String DPM_URL_FILE = "dpm-url.txt";

  public static final int HTTP_PERMANENT_REDIRECT_STATUS = 308; // reference: https://tools.ietf.org/html/rfc7538
  public static final String HTTP_LOCATION_HEADER = "Location";
  private static final Map INVALID_PERMANENT_REDIR = ImmutableMap.of();
  private static final Map TOO_MANY_PERMANENT_REDIR = ImmutableMap.of();

  volatile String dpmBaseUrl;
  volatile RestClient.Builder registerClientBuilder;
  volatile RestClient.Builder userAuthClientBuilder;
  volatile RestClient.Builder appAuthClientBuilder;
  private Configuration conf;
  private String appToken;
  private String componentId;
  private volatile int connTimeout;
  private int dpmRegistrationMaxRetryAttempts;
  private volatile boolean serviceActive;

  @Override
  public void setConfiguration(Configuration conf) {
    this.conf = conf;
    super.setConfiguration(conf);

    createLoginLogoutUrls(getExternalDpmBaseUrl());
    dpmBaseUrl = getDpmBaseUrl();
    createRestClientBuilders(dpmBaseUrl);

    componentId = conf.get(SECURITY_SERVICE_COMPONENT_ID_CONFIG, null);
    appToken = conf.get(SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, null);
    connTimeout = conf.get(SECURITY_SERVICE_CONNECTION_TIMEOUT_CONFIG, DEFAULT_SECURITY_SERVICE_CONNECTION_TIMEOUT);
    dpmRegistrationMaxRetryAttempts = conf.get(DPM_REGISTRATION_RETRY_ATTEMPTS,
        DPM_REGISTRATION_RETRY_ATTEMPTS_DEFAULT);
  }

  protected String getDpmBaseUrl() {
    String securityAppUrl = conf.get(DPM_APP_SECURITY_URL_CONFIG, null);
    if (securityAppUrl == null || securityAppUrl.isEmpty()) {
      securityAppUrl = getExternalDpmBaseUrl();
      LOG.debug("Security App URL was NULL and has been set to dpm.base.url: {}", securityAppUrl);
    }
    return getValidURL(securityAppUrl).trim();
  }

  protected String getExternalDpmBaseUrl() {
    return getValidURL(conf.get(DPM_BASE_URL_CONFIG, DPM_BASE_URL_DEFAULT));
  }

  synchronized void createLoginLogoutUrls(String externalBaseUrl) {
    final String externalUrl = getValidURL(externalBaseUrl) + "security";
    setLoginPageUrl(externalUrl + "/login");
    setLogoutUrl(externalUrl + "/_logout");
  }

  @VisibleForTesting
  synchronized void createRestClientBuilders(String dpmBaseUrl) {
    dpmBaseUrl = getValidURL(dpmBaseUrl) + "security";

    Utils.checkArgument(
        dpmBaseUrl.toLowerCase().startsWith("http:") || dpmBaseUrl.toLowerCase().startsWith("https:"),
        Utils.formatL("Security service base URL must be HTTP/HTTPS '{}'", dpmBaseUrl)
    );
    if (dpmBaseUrl.toLowerCase().startsWith("http://")) {
      LOG.warn("Security service base URL is not secure '{}'", dpmBaseUrl);
    }
    registerClientBuilder = RestClient.builder(dpmBaseUrl)
        .csrf(true)
        .json(true)
        .path("public-rest/v1/components/registration")
        .timeout(connTimeout);
    userAuthClientBuilder = RestClient.builder(dpmBaseUrl)
        .csrf(true)
        .json(true)
        .path("rest/v1/validateAuthToken/user")
        .timeout(connTimeout);
    appAuthClientBuilder = RestClient.builder(dpmBaseUrl)
        .csrf(true)
        .json(true)
        .path("rest/v1/validateAuthToken/component")
        .timeout(connTimeout);
  }

  @VisibleForTesting
  public RestClient.Builder getRegisterClientBuilder() {
    return registerClientBuilder;
  }

  @VisibleForTesting
  public RestClient.Builder getUserAuthClientBuilder() {
    return userAuthClientBuilder;
  }

  @VisibleForTesting
  public RestClient.Builder getAppAuthClientBuilder() {
    return appAuthClientBuilder;
  }

  @VisibleForTesting
  void sleep(int secs) {
    try {
      Thread.sleep(secs * 1000);
    } catch (InterruptedException ex) {
      String msg = "Interrupted while attempting DPM registration";
      LOG.error(msg);
      throw new RuntimeException(msg, ex);
    }
  }

  void updateConnectionTimeout(RestClient.Response response) {
    String timeout = response.getHeader(SSOConstants.X_APP_CONNECTION_TIMEOUT);
    connTimeout = (timeout == null) ? connTimeout: Integer.parseInt(timeout);
  }

  protected boolean checkServiceActive() {
    boolean active;
    try {
      URL url = new URL(getLoginPageUrl());
      HttpURLConnection httpURLConnection = ((HttpURLConnection)url.openConnection());
      httpURLConnection.setConnectTimeout(connTimeout);
      httpURLConnection.setReadTimeout(connTimeout);
      int status = httpURLConnection.getResponseCode();
      active = status == HttpURLConnection.HTTP_OK;
      if (!active) {
        LOG.warn("DPM reachable but returning '{}' HTTP status on login", status);
      }
    } catch (IOException ex) {
      LOG.warn("DPM not reachable: {}", ex.toString());
      active = false;
    }
    LOG.debug("DPM current status '{}'", (active) ? "ACTIVE" : "NON ACTIVE");
    return active;
  }

  public boolean isServiceActive(boolean checkNow) {
    if (checkNow) {
      serviceActive = checkServiceActive();
    }
    return serviceActive;
  }

  @Override
  public void register(Map<String, String> attributes) {
    if (appToken.isEmpty() || componentId.isEmpty()) {
      if (appToken.isEmpty()) {
        LOG.warn("Skipping component registration to DPM, application auth token is not set");
      }
      if (componentId.isEmpty()) {
        LOG.warn("Skipping component registration to DPM, component ID is not set");
      }
      throw new RuntimeException("Registration to DPM not done, missing component ID or app auth token");
    } else {
      LOG.debug("Doing component ID '{}' registration with DPM", componentId);

      Map<String, Object> registrationData = new HashMap<>();
      registrationData.put("authToken", appToken);
      registrationData.put("componentId", componentId);
      registrationData.put("attributes", attributes);

      int delaySecs = 1;
      int attempts = 0;
      boolean registered = false;

      //When Load Balancer(HAProxy or ELB) is used, it will take couple of seconds for load balancer to access
      //security service. So we are retrying registration couple of times until server is accessible via load balancer.
      while (attempts < dpmRegistrationMaxRetryAttempts) {
        if (attempts > 0) {
          delaySecs = delaySecs * 2;
          delaySecs = Math.min(delaySecs, 16);
          LOG.warn("DPM registration attempt '{}', waiting for '{}' seconds before retrying ...", attempts, delaySecs);
          sleep(delaySecs);
        }
        attempts++;
        try {
          RestClient restClient = getRegisterClientBuilder().build();
          RestClient.Response response = restClient.post(registrationData);
          int status = response.getStatus();
          if (status == HttpURLConnection.HTTP_OK) {
            updateConnectionTimeout(response);
            processRegistrationResponse(response);
            LOG.info("Registered with DPM");
            registered = true;
            break;
          } else if (status == HttpURLConnection.HTTP_UNAVAILABLE) {
            LOG.warn("DPM Registration unavailable");
          } else if (status == HTTP_PERMANENT_REDIRECT_STATUS) {
            String newLocation = response.getHeader(HTTP_LOCATION_HEADER);
            LOG.info("Received a MOVED_PERMANENTLY to '{}'", newLocation);
            if (newLocation == null) {
              throw new ForbiddenException(INVALID_PERMANENT_REDIR);
            }
            updateDpmBaseUrl(MovedException.extractBaseUrl(newLocation));
          } else if (status == HttpURLConnection.HTTP_FORBIDDEN ||
              status == HttpURLConnection.HTTP_BAD_REQUEST) {
            throw new RuntimeException(Utils.format(
                "Failed registration for component ID '{}': {}",
                componentId,
                response.getError()
            ));
          } else {
            LOG.warn("Failed to registered to DPM, HTTP status '{}': {}", status, response.getError());
            break;
          }
        } catch (IOException ex) {
          LOG.warn("DPM Registration failed: {}", ex.toString());
        }
      }
      if (registered) {
        clearCaches();
        serviceActive = true;
      } else {
        LOG.warn("DPM registration failed after '{}' attempts", attempts);
      }
    }
  }

  private void processRegistrationResponse(RestClient.Response response) throws IOException {
    if(!response.haveData()) {
      LOG.debug("Received empty registration response from Control Hub");
      return;
    }

    if(registrationResponseDelegate != null) {
      RegistrationResponseJson registrationResponse = response.getData(RegistrationResponseJson.class);
      registrationResponseDelegate.processRegistrationResponse(registrationResponse);
    }
  }

  public void setComponentId(String componentId) {
    componentId = (componentId != null) ? componentId.trim() : null;
    Utils.checkArgument(componentId != null && !componentId.isEmpty(), "Component ID cannot be NULL or empty");
    this.componentId = componentId;
    registerClientBuilder.componentId(componentId);
    userAuthClientBuilder.componentId(componentId);
    appAuthClientBuilder.componentId(componentId);
  }

  public void setApplicationAuthToken(String appToken) {
    appToken = (appToken != null) ? appToken.trim() : null;
    this.appToken = appToken;
    registerClientBuilder.appAuthToken(appToken);
    userAuthClientBuilder.appAuthToken(appToken);
    appAuthClientBuilder.appAuthToken(appToken);
  }

  private boolean checkServiceActiveIfInActive() {
    if (!serviceActive) {
      serviceActive = checkServiceActive();
    }
    return serviceActive;
  }

  protected SSOPrincipal validateUserTokenWithSecurityService(String userAuthToken)
      throws ForbiddenException {
    return validateTokenWithSecurityService(() ->
        {
          ValidateUserAuthTokenJson authTokenJson = new ValidateUserAuthTokenJson();
          authTokenJson.setAuthToken(userAuthToken);
          RestClient restClient = getUserAuthClientBuilder().build();
          return restClient.post(authTokenJson);
        },
        userAuthToken,
        "User session");
  }

  protected SSOPrincipal validateAppTokenWithSecurityService(String authToken, String componentId)
      throws ForbiddenException {
    return validateTokenWithSecurityService(() ->
        {
          ValidateComponentAuthTokenJson authTokenJson = new ValidateComponentAuthTokenJson();
          authTokenJson.setComponentId(componentId);
          authTokenJson.setAuthToken(authToken);
          RestClient restClient = getAppAuthClientBuilder().build();
          return restClient.post(authTokenJson);
        },
        authToken,
        "API token '" + componentId + "'");
  }

  protected SSOPrincipal validateTokenWithSecurityService(
      Callable<RestClient.Response> restCall,
      String authToken,
      String forMessages
  ) throws ForbiddenException {
    Utils.checkState(checkServiceActiveIfInActive(), "Security service not active");
    SSOPrincipalJson principal = null;

    try {
      int tries = 2; // we only loop  if there is permanent redirect
      do {
        RestClient.Response response = restCall.call();
        int status = response.getStatus();
        if (status == HttpURLConnection.HTTP_OK) {
          updateConnectionTimeout(response);
          principal = response.getData(SSOPrincipalJson.class);
        } else if (status == HttpURLConnection.HTTP_FORBIDDEN) {
          throw new ForbiddenException(response.getError());
        } else if (status == HTTP_PERMANENT_REDIRECT_STATUS) {
          String newLocation = response.getHeader(HTTP_LOCATION_HEADER);
          LOG.info("Received a MOVED_PERMANENTLY to '{}'", newLocation);
          if (newLocation == null) {
            throw new ForbiddenException(INVALID_PERMANENT_REDIR);
          }
          updateDpmBaseUrl(MovedException.extractBaseUrl(newLocation));
        } else {
          throw new RuntimeException(Utils.format(
              "Could not validate {}, HTTP status '{}' message: {}",
              forMessages,
              status,
              response.getError()
          ));
        }

      } while (principal == null && tries-- > 0);
      if (principal == null && tries == 0) {
        throw new ForbiddenException(TOO_MANY_PERMANENT_REDIR);
      }
    } catch (Exception ex){
      LOG.warn("Could not do {} validation, going inactive: {}", forMessages, ex.toString());
      serviceActive = false;
      throw new RuntimeException(Utils.format("Could not connect to security service: {}", ex), ex);
    }
    if (principal != null) {
      principal.setTokenStr(authToken);
      principal.lock();
      LOG.debug("Validated token {} for '{}'", forMessages, principal.getPrincipalId());
    }
    return principal;
  }

  /**
   * Sub-classes may override this method to disable the updating functionality.
   */
  protected void updateDpmBaseUrl(String baseUrl) {
    Utils.checkState(conf.get(DPM_APP_SECURITY_URL_CONFIG, null) == null,
        "Automatic update of dpm.base.url can only be done if dpm.app.<APP>.url configs are not defined");
    persistNewDpmBaseUrl(baseUrl);
    createLoginLogoutUrls(baseUrl);
    createRestClientBuilders(baseUrl);
  }

  /**
   * Only used by this class.
   */
  @VisibleForTesting
  void persistNewDpmBaseUrl(String baseUrl) {
    persistNewDpmBaseUrl(conf.getFilRefsBaseDir(), baseUrl);
  }

    /**
     * The engine configuration must be 'dpm.base.url=@dpm-url.txt@' for this work correctly.
     * <p/>
     * This static method is to be used by other classes.
     */
  //saves new DPM base URL in predefine DPM_URL_FILE (etc/dpm-url.txt)
  public static void persistNewDpmBaseUrl(File dir, String baseUrl) {
    File dpmUrlFile = new File(dir, DPM_URL_FILE);
    try (Writer w = new FileWriter(dpmUrlFile)) {
      w.write(baseUrl);
    } catch (IOException ex) {
      throw new RuntimeException(Utils.format("Could not write new DPM base URL to '{}', error: {}", dpmUrlFile, ex), ex);
    }
    LOG.info("Updated DPM base URL to {}", baseUrl);
  }

  public static String getValidURL(String url) {
    if (!url.endsWith("/")) {
      url += "/";
    }
    return url;
  }

  @VisibleForTesting
  int getConnectionTimeout() {
    return connTimeout;
  }

  public DpmClientInfo getDpmClientInfo() {
    return new DpmClientInfo() {
      @Override
      public String getDpmBaseUrl() {
        return dpmBaseUrl;
      }

      @Override
      public Map<String, String> getHeaders() {
        return ImmutableMap.of(
            SSOConstants.X_APP_COMPONENT_ID.toLowerCase(),
            componentId,
            SSOConstants.X_APP_AUTH_TOKEN.toLowerCase(),
            appToken
        );
      }

      @Override
      public void setDpmBaseUrl(String dpmBaseUrl) {
        persistNewDpmBaseUrl(dpmBaseUrl);
      }
    };
  }
}
