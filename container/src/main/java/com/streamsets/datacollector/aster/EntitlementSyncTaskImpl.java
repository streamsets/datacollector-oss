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
package com.streamsets.datacollector.aster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.aster.AsterRestClient;
import com.streamsets.lib.security.http.aster.AsterService;
import com.streamsets.lib.security.http.aster.AsterServiceProvider;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;

public class EntitlementSyncTaskImpl extends AbstractTask implements EntitlementSyncTask {
  private static final Logger LOG = LoggerFactory.getLogger(EntitlementSyncTaskImpl.class);
  @VisibleForTesting
  static final String DATA = "data";
  @VisibleForTesting
  static final String ACTIVATION_CODE = "activationCode";
  @VisibleForTesting
  static final String ACTIVATION_ENDPOINT_PATH = "/api/entitlements/v3/activations";

  private final Activation activation;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final Configuration appConfig;

  @Inject
  public EntitlementSyncTaskImpl(
      Activation activation,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      Configuration appConfig) {
    super("EntitlementSyncTask");
    this.activation = activation;
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.appConfig = appConfig;
  }

  @Override
  protected void initTask() {
    super.initTask();
    if (AsterServiceProvider.isEnabled(appConfig) && AsterServiceProvider.getInstance().getService() != null) {
      AsterServiceProvider.getInstance().getService().registerHooks(Collections.singletonList(this));
    } else {
      LOG.debug("Aster is not enabled, skipping registration of EntitlementSync hook");
    }
  }

  @Override
  protected void runTask() {
    syncEntitlement();
  }

  @Override
  public void onSuccessfulRegistration(String originatedURL) {
    syncEntitlement();
  }

  @Override
  public synchronized boolean syncEntitlement() {
    if (runtimeInfo.isClusterSlave()) {
      LOG.debug("Slave mode, skipping EntitlementSync");
      return false;
    }
    if (!activation.isEnabled()) {
      LOG.debug("Activation not enabled, skipping EntitlementSync");
      return false;
    }
    if (activation.getInfo().isValid()) {
      LOG.debug("Activation already valid, skipping EntitlementSync");
      return false;
    }
    if (!AsterServiceProvider.isEnabled(appConfig)) {
      LOG.debug("Aster is disabled, skipping EntitlementSync");
      return false;
    }
    AsterService aster = getAsterService();
    if (!aster.getRestClient().hasTokens()) {
      LOG.debug("No credentials available, skipping EntitlementSync");
      return false;
    }

    String asterUrl = aster.getConfig().getBaseUrl();
    try {
      AsterRestClient.Response<Map<String, Object>> response =
          postToGetEntitlementUrl(
              asterUrl + ACTIVATION_ENDPOINT_PATH,
              getEntitlementArguments()
          );

      if (response.getStatusCode() != HttpStatus.SC_CREATED) {
        LOG.warn("Unable to refresh entitlement from {}, response code {}",
            aster.getConfig().getBaseUrl() + ACTIVATION_ENDPOINT_PATH,
            response.getStatusCode()
        );
        return false;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> responseData = response.getBody();
      String entitlement = getEntitlementFromResponse(responseData);
      if (entitlement != null) {
        LOG.info("Updating entitlement from {}", asterUrl + ACTIVATION_ENDPOINT_PATH);
        activation.setActivationKey(entitlement);
        return true;
      } else {
        LOG.warn("No valid entitlement returned from {}", asterUrl + ACTIVATION_ENDPOINT_PATH);
        return false;
      }
    } catch (RuntimeException e) {
      LOG.warn(e.getMessage(), e);
    }
    return false;
  }

  @VisibleForTesting
  AsterService getAsterService() {
    return AsterServiceProvider.getInstance().getService();
  }

  private Map<String, Object> getEntitlementArguments() {
    return ImmutableMap.of(DATA, ImmutableMap.of(
        "productId", runtimeInfo.getId(),
        "productType", runtimeInfo.getProductName().equals(RuntimeInfo.SDC_PRODUCT) ?
            "DATA_COLLECTOR" : "TRANSFORMER",
        "productUrl", runtimeInfo.getBaseHttpUrl(),
        "productVersion", buildInfo.getVersion()
        ),
        "version", 2
    );
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  AsterRestClient.Response<Map<String, Object>> postToGetEntitlementUrl(
      String entitlementUrl,
      Map<String, Object> data) {
    AsterService service = getAsterService();
    AsterRestClient.Request<Map<String, Object>> request =
        new AsterRestClient.Request<Map<String, Object>>()
            .setResourcePath(entitlementUrl)
            .setRequestType(AsterRestClient.RequestType.POST)
            .setPayloadClass((Class)Map.class)
            .setPayload(data);

    return service.getRestClient().doRestCall(request);
  }

  private String getEntitlementFromResponse(Map<String, Object> responseData) {
    if (responseData.containsKey(DATA) && responseData.get(DATA) instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> unwrappedData = (Map<String, Object>) responseData.get(DATA);
      if (unwrappedData.containsKey(ACTIVATION_CODE) && unwrappedData.get(ACTIVATION_CODE) instanceof String) {
        return (String) unwrappedData.get(ACTIVATION_CODE);
      } else {
        LOG.warn("Activation code missing from response from Aster at path " + ACTIVATION_ENDPOINT_PATH);
      }
    } else {
      LOG.trace("No Data in response from Aster at path " + ACTIVATION_ENDPOINT_PATH);
    }
    return null;
  }
}
