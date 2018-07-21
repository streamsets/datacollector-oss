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
package com.streamsets.datacollector.cli.sch;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.DPMInfoJson;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Methods to "administer" SCH.
 *
 * The current implementation is pretty much just moved from original REST end points and should be further refactored
 * eventually into some sort of proper admin object.
 */
public class SchAdmin {
  private static final String APP_TOKEN_FILE = "application-token.txt";
  private static final String APP_TOKEN_FILE_PROP_VAL = "@application-token.txt@";

  public static void enableDPM(DPMInfoJson dpmInfo, RuntimeInfo runtimeInfo, Configuration configuration) throws IOException {
    Utils.checkNotNull(dpmInfo, "DPMInfo");

    String dpmBaseURL = dpmInfo.getBaseURL();
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    // Since we support enabling/Disabling DPM, first check if token already exists for the given DPM URL.
    // If token exists skip first 3 steps
    String currentDPMBaseURL = configuration.get(RemoteSSOService.DPM_BASE_URL_CONFIG, "");
    String currentAppAuthToken = configuration.get(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "").trim();
    if (!currentDPMBaseURL.equals(dpmBaseURL) ||  currentAppAuthToken.length() == 0) {
      // 1. Login to DPM to get user auth token
      Response response = null;
      try {
        Map<String, String> loginJson = new HashMap<>();
        loginJson.put("userName", dpmInfo.getUserID());
        loginJson.put("password", dpmInfo.getUserPassword());
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/public-rest/v1/authentication/login")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .post(Entity.json(loginJson));
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
          throw new RuntimeException(Utils.format("DPM Login failed, status code '{}': {}",
              response.getStatus(),
              response.readEntity(String.class)
          ));
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }

      String userAuthToken = response.getHeaderString(SSOConstants.X_USER_AUTH_TOKEN);
      String appAuthToken = null;

      // 2. Create Data Collector application token
      try {
        Map<String, Object> newComponentJson = new HashMap<>();
        newComponentJson.put("organization", dpmInfo.getOrganization());
        newComponentJson.put("componentType", "dc");
        newComponentJson.put("numberOfComponents", 1);
        newComponentJson.put("active", true);
        response = ClientBuilder.newClient()
            .target(dpmBaseURL + "/security/rest/v1/organization/" + dpmInfo.getOrganization() + "/components")
            .register(new CsrfProtectionFilter("CSRF"))
            .request()
            .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
            .put(Entity.json(newComponentJson));
        if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
          throw new RuntimeException(Utils.format("DPM Create Application Token failed, status code '{}': {}",
              response.getStatus(),
              response.readEntity(String.class)
          ));
        }

        List<Map<String, Object>> newComponent = response.readEntity(new GenericType<List<Map<String,Object>>>() {});
        if (newComponent.size() > 0) {
          appAuthToken = (String) newComponent.get(0).get("fullAuthToken");
        } else {
          throw new RuntimeException("DPM Create Application Token failed: No token data from DPM Server.");
        }

      } finally {
        if (response != null) {
          response.close();
        }
        // Logout from DPM
        try {
          response = ClientBuilder.newClient()
              .target(dpmBaseURL + "/security/_logout")
              .register(new CsrfProtectionFilter("CSRF"))
              .request()
              .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
              .cookie(SSOConstants.AUTHENTICATION_COOKIE_PREFIX + "LOGIN", userAuthToken)
              .get();
        } finally {
          if (response != null) {
            response.close();
          }
        }
      }

      // 3. Update App Token file
      DataStore dataStore = new DataStore(new File(runtimeInfo.getConfigDir(), APP_TOKEN_FILE));
      try (OutputStream os = dataStore.getOutputStream()) {
        IOUtils.write(appAuthToken, os);
        dataStore.commit(os);
      } finally {
        dataStore.release();
      }
    }

    // 4. Update dpm.properties file
    try {
      FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
          new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
              .configure(new Parameters().properties()
                  .setFileName(runtimeInfo.getConfigDir() + "/dpm.properties")
                  .setThrowExceptionOnMissing(true)
                  .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                  .setIncludesAllowed(false));
      PropertiesConfiguration config = null;
      config = builder.getConfiguration();

      config.setProperty(RemoteSSOService.DPM_ENABLED, "true");
      config.setProperty(RemoteSSOService.DPM_BASE_URL_CONFIG, dpmBaseURL);
      config.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, APP_TOKEN_FILE_PROP_VAL);
      if (dpmInfo.getLabels() != null && dpmInfo.getLabels().size() > 0) {
        config.setProperty(RemoteEventHandlerTask.REMOTE_JOB_LABELS, StringUtils.join(dpmInfo.getLabels(), ','));
      } else {
        config.setProperty(RemoteEventHandlerTask.REMOTE_JOB_LABELS, "");
      }

      builder.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(Utils.format("Updating dpm.properties file failed: {}", e.getMessage()), e);
    }
  }

  public static void disableDPM(String username, String password, String organizationId, RuntimeInfo runtimeInfo, Configuration configuration) throws IOException {
    String dpmBaseURL = configuration.get(RemoteSSOService.DPM_BASE_URL_CONFIG, "");
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    // 1. Login to DPM to get user auth token
    Response response = null;
    try {
      Map<String, String> loginJson = new HashMap<>();
      loginJson.put("userName", username);
      loginJson.put("password", password);
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/public-rest/v1/authentication/login")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .post(Entity.json(loginJson));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format("DPM Login failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    String userAuthToken = response.getHeaderString(SSOConstants.X_USER_AUTH_TOKEN);
    disableDPM(userAuthToken, organizationId, runtimeInfo, configuration);
  }

  public static void disableDPM(String userAuthToken, String organizationId, RuntimeInfo runtimeInfo, Configuration configuration) throws IOException {
    // check if DPM enabled
    if (!runtimeInfo.isDPMEnabled()) {
      throw new RuntimeException("disableDPM is supported only when DPM is enabled");
    }

    String dpmBaseURL = configuration.get(RemoteSSOService.DPM_BASE_URL_CONFIG, "");
    if (dpmBaseURL.endsWith("/")) {
      dpmBaseURL = dpmBaseURL.substring(0, dpmBaseURL.length() - 1);
    }

    String componentId = runtimeInfo.getId();

    // 1. User authentication token has been retrieved already

    // 2. Deactivate Data Collector System Component
    Response response = null;
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/rest/v1/organization/" + organizationId + "/components/deactivate")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .post(Entity.json(ImmutableList.of(componentId)));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            " Deactivate Data Collector System Component from DPM failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 3. Delete Data Collector System Component
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/security/rest/v1/organization/" + organizationId + "/components/delete")
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .post(Entity.json(ImmutableList.of(componentId)));
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            " Deactivate Data Collector System Component from DPM failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 4. Delete from Job Runner SDC list
    try {
      response = ClientBuilder.newClient()
          .target(dpmBaseURL + "/jobrunner/rest/v1/sdc/" + componentId)
          .register(new CsrfProtectionFilter("CSRF"))
          .request()
          .header(SSOConstants.X_USER_AUTH_TOKEN, userAuthToken)
          .header(SSOConstants.X_REST_CALL, true)
          .delete();
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new RuntimeException(Utils.format(
            "Delete from DPM Job Runner SDC list failed, status code '{}': {}",
            response.getStatus(),
            response.readEntity(String.class)
        ));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }

    // 5. Update App Token file
    DataStore dataStore = new DataStore(new File(runtimeInfo.getConfigDir(), APP_TOKEN_FILE));
    try (OutputStream os = dataStore.getOutputStream()) {
      IOUtils.write("", os);
      dataStore.commit(os);
    } finally {
      dataStore.release();
    }

    // 4. Update dpm.properties file
    try {
      FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
          new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
              .configure(new Parameters().properties()
                  .setFileName(runtimeInfo.getConfigDir() + "/dpm.properties")
                  .setThrowExceptionOnMissing(true)
                  .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                  .setIncludesAllowed(false));
      PropertiesConfiguration config = null;
      config = builder.getConfiguration();
      config.setProperty(RemoteSSOService.DPM_ENABLED, "false");
      builder.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(Utils.format("Updating dpm.properties file failed: {}", e.getMessage()), e);
    }
  }
}
