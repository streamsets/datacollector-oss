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
import com.streamsets.datacollector.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

public class FailoverSSOService implements SSOService {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverSSOService.class);

  private final RemoteSSOService remoteService;
  private final DisconnectedSSOService disconnectedService;
  private volatile SSOService activeService;
  private volatile boolean recoveryInProgress;
  private Map<String, String> registrationAttributes;

  public FailoverSSOService(RemoteSSOService remoteService, DisconnectedSSOService disconnectedService) {
    this.remoteService = remoteService;
    this.disconnectedService = disconnectedService;
    activeService = remoteService;
  }

  @VisibleForTesting
  SSOService getActiveService() {
    return activeService;
  }

  @VisibleForTesting
  boolean isRecoveryInProgress() {
    return recoveryInProgress;
  }

  @VisibleForTesting
  void setRecoveryInProgress(boolean inProgress) {
    recoveryInProgress = inProgress;
  }

  @Override
  public void setDelegateTo(SSOService ssoService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SSOService getDelegateTo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfiguration(Configuration configuration) {
    remoteService.setConfiguration(configuration);
    disconnectedService.setConfiguration(configuration);
  }

  @VisibleForTesting
  void failoverIfRemoteNotActive(boolean checkNow) {
    if (activeService == remoteService) {
      if (!remoteService.isServiceActive(checkNow)) {
        LOG.info("RemoteSSOService is not active, changing to disconnected mode");
        activeService = disconnectedService;
        activeService.register(registrationAttributes);
      }
    }
    if (activeService == disconnectedService) {
      if (!isRecoveryInProgress()) {
        boolean triggerRecovery = false;
        //we can do this because recoveryFuture is a volatile
        synchronized (this) {
          if (!isRecoveryInProgress()) {
            setRecoveryInProgress(true);
            triggerRecovery = true;
          }
        }
        if (triggerRecovery) {
          LOG.debug("RemoteSSOService starting a recovery attempt");
          runRecovery();
        }
      }
    }
  }

  @VisibleForTesting
  Thread runRecovery() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          remoteService.register(registrationAttributes);
          if (remoteService.isServiceActive(false)) {
            LOG.info("RemoteSSOService is active, changing to connected mode");
            activeService = remoteService;
          } else {
            LOG.debug("RemoteSSOService still not active, remaining in disconnected mode");
          }
        } catch (Exception ex) {
          LOG.error("Error while trying to activate RemoteSSOService: {}", ex.toString(), ex);
        }
        setRecoveryInProgress(false);
      }
    });
    thread.setDaemon(true);
    thread.setName("RemoteSSOService-recovery");
    thread.start();
    return thread;
  }

  @Override
  public void register(Map<String, String> attributes) {
    registrationAttributes = attributes;
    remoteService.register(attributes);
    failoverIfRemoteNotActive(false);
  }

  @Override
  public String createRedirectToLoginUrl(
      String requestUrl, boolean duplicateRedirect, HttpServletRequest req, HttpServletResponse res
  ) {
    failoverIfRemoteNotActive(true);
    return getActiveService().createRedirectToLoginUrl(requestUrl, duplicateRedirect, req, res);
  }

  @Override
  public String getLoginPageUrl() {
    failoverIfRemoteNotActive(false);
    return getActiveService().getLoginPageUrl();
  }

  @Override
  public String getLogoutUrl() {
    failoverIfRemoteNotActive(false);
    return getActiveService().getLogoutUrl();
  }

  @Override
  public SSOPrincipal validateUserToken(String authToken) {
    failoverIfRemoteNotActive(false);
    return getActiveService().validateUserToken(authToken);
  }

  @Override
  public boolean invalidateUserToken(String authToken) {
    failoverIfRemoteNotActive(false);
    return getActiveService().invalidateUserToken(authToken);
  }

  @Override
  public SSOPrincipal validateAppToken(String authToken, String componentId) {
    failoverIfRemoteNotActive(false);
    return getActiveService().validateAppToken(authToken, componentId);
  }

  @Override
  public boolean invalidateAppToken(String authToken) {
    failoverIfRemoteNotActive(false);
    return getActiveService().invalidateAppToken(authToken);
  }

  @Override
  public void clearCaches() {
    failoverIfRemoteNotActive(false);
    getActiveService().clearCaches();
  }

  @Override
  public void setRegistrationResponseDelegate(RegistrationResponseDelegate delegate) {
    remoteService.setRegistrationResponseDelegate(delegate);
    disconnectedService.setRegistrationResponseDelegate(delegate);
  }

}
