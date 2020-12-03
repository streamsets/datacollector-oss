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
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.aster.AsterRestClient;
import com.streamsets.lib.security.http.aster.AsterService;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class EntitlementSyncTaskImpl extends AbstractTask implements EntitlementSyncTask {
  private static final Logger LOG = LoggerFactory.getLogger(EntitlementSyncTaskImpl.class);
  @VisibleForTesting
  static final String DATA = "data";
  @VisibleForTesting
  static final String ACTIVATION_CODE = "activationCode";
  @VisibleForTesting
  static final String ACTIVATION_ENDPOINT_PATH = "/api/entitlements/v3/activations";
  @VisibleForTesting
  static final String SYNC_MAX_RETRY_WINDOW_CONFIG = "entitlement.sync.max.retry.window.ms";
  @VisibleForTesting
  static final long SYNC_MAX_RETRY_WINDOW_DEFAULT = TimeUnit.MINUTES.toMillis(3);
  @VisibleForTesting
  static final String SYNC_RETRY_INITIAL_BACKOFF_CONFIG = "entitlement.sync.initial.backoff.ms";
  @VisibleForTesting
  static final long SYNC_RETRY_INITIAL_BACKOFF_DEFAULT = TimeUnit.SECONDS.toMillis(1);
  @VisibleForTesting
  static final String SYNC_TIMEOUT_CONFIG = "entitlement.sync.timeout.ms";
  @VisibleForTesting
  static final int SYNC_TIMEOUT_DEFAULT = (int) TimeUnit.SECONDS.toMillis(30);

  private static final Set<Integer> STATUS_CODES_TO_RETRY = ImmutableSet.of(500, 502, 503, 504);

  private final Activation activation;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final Configuration appConfig;
  private final AsterContext asterContext;
  private final StatsCollector statsCollector;
  private final long syncMaxRetryWindow;
  private final long syncRetryInitialBackoff;
  private final int syncTimeout;

  @Inject
  public EntitlementSyncTaskImpl(
      Activation activation,
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      Configuration appConfig,
      AsterContext asterContext,
      StatsCollector statsCollector
      ) {
    super("EntitlementSyncTask");
    this.activation = activation;
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.appConfig = appConfig;
    this.asterContext = asterContext;
    this.statsCollector = statsCollector;
    this.syncMaxRetryWindow = appConfig.get(SYNC_MAX_RETRY_WINDOW_CONFIG, SYNC_MAX_RETRY_WINDOW_DEFAULT);
    this.syncRetryInitialBackoff = appConfig.get(SYNC_RETRY_INITIAL_BACKOFF_CONFIG, SYNC_RETRY_INITIAL_BACKOFF_DEFAULT);
    this.syncTimeout = appConfig.get(SYNC_TIMEOUT_CONFIG, SYNC_TIMEOUT_DEFAULT);
  }

  @Override
  protected void initTask() {
    super.initTask();
    if (asterContext.isEnabled()) {
      asterContext.getService().registerHooks(Collections.singletonList(this));
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
    if (!asterContext.isEnabled()) {
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
        if (!statsCollector.isOpted()) {
          statsCollector.setActive(true);
        }
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
    return asterContext.getService();
  }

  private Map<String, Object> getEntitlementArguments() {
    return ImmutableMap.of(DATA, ImmutableMap.of(
        "productId", runtimeInfo.getId(),
        "productType", runtimeInfo.getProductName().equals(RuntimeInfo.SDC_PRODUCT) ?
            "DATA_COLLECTOR" : "TRANSFORMER",
        "productUrl", runtimeInfo.getBaseHttpUrl(true),
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
            .setPayload(data)
            .setTimeout(syncTimeout);

    long startTime = getTime();
    long nextSleep = syncRetryInitialBackoff;
    AsterRestClient.Response<Map<String, Object>> response = null;
    RuntimeException exception = null;
    while (true) {
      try {
        response = service.getRestClient().doRestCall(request);
        int statusCode = response.getStatusCode();
        if (!STATUS_CODES_TO_RETRY.contains(statusCode)) {
          return response;
        }
        LOG.warn("Retrying entitlement sync after failure with status code {} and message {}",
            statusCode,
            response.getStatusMessage());
        exception = null;
      } catch (RuntimeException e) {
        boolean retry = false;
        if (e.getCause() != null && e.getCause() instanceof SocketTimeoutException) {
          retry = true;
          LOG.warn("Retrying entitlement sync after socket timeout: " + e.getMessage());
          exception = e;
          response = null;
        } else if (getStatusCode(e).map(STATUS_CODES_TO_RETRY::contains).orElse(false)) {
          retry = true;
          LOG.warn("Retrying entitlement sync after: {}", e.getMessage());
          exception = e;
          response = null;
        }
        if (!retry) {
          throw e;
        }
      }
      long timeAfterAttempt = getTime();
      long nextRun = timeAfterAttempt + nextSleep;
      if (nextRun > startTime + syncMaxRetryWindow) {
        // next run would start outside the retry window
        LOG.warn("Next retry would be outside the retry window, giving up");
        break;
      }
      LOG.debug("Sleeping for {} ms between retries", nextSleep);
      if (!sleep(nextSleep)) {
        LOG.debug("Entitlement Sync sleep interrupted");
        break;
      }
      nextSleep *= 2; // exponential backoff
    }
    if (exception != null) {
      throw exception;
    }
    return response;
  }

  @VisibleForTesting
  long getTime() {
    // allows easy mocking
    return System.currentTimeMillis();
  }

  /**
   * @param millis sleep duration in millis
   * @return whether slept for full duration
   */
  @VisibleForTesting
  boolean sleep(long millis) {
    // allows easy mocking
    return ThreadUtil.sleep(millis);
  }

  @VisibleForTesting
  Optional<Integer> getStatusCode(RuntimeException e) {
    Throwable nextE = e;
    while (true) {
      // check if it is the expected class type. The actual class is not visible in this classloader, so check name
      if (nextE.getClass().getSimpleName().equals("HttpServerErrorException")) {
        if (nextE.getMessage().matches("[0-9][0-9][0-9] .*")) { // Matches syntax of HttpServerErrorException
          return Optional.of(Integer.valueOf(nextE.getMessage().substring(0, 3)));
        }
        break;
      }

      if (nextE.getCause() == null || nextE.getCause() == nextE) {
        break;
      }
      nextE = nextE.getCause();
    }
    return Optional.empty();
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
