/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.callback;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import java.util.Map;


public class CallbackServerMetricsEventListener implements MetricsEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackServerMetricsEventListener.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();
  private final String name;
  private final String rev;
  private final String user;
  private final RuntimeInfo runtimeInfo;
  private final String callbackServerURL;
  private final String sdcClusterToken;
  private final String sdcSlaveToken;

  public CallbackServerMetricsEventListener(String user, String name, String rev,
    RuntimeInfo runtimeInfo, String callbackServerURL, String sdcClusterToken, String sdcSlaveToken) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.runtimeInfo = runtimeInfo;
    this.callbackServerURL = callbackServerURL;
    Utils.checkNotNull(sdcClusterToken, "SDC Cluster Token");
    this.sdcClusterToken = sdcClusterToken;
    Utils.checkNotNull(sdcSlaveToken, "SDC Slave Token");
    this.sdcSlaveToken = sdcSlaveToken;
  }

  @Override
  public void notification(String metrics) {
    try {
      Map<String, String> authenticationToken = runtimeInfo.getAuthenticationTokens();
      CallbackInfo callbackInfo =
        new CallbackInfo(user, name, rev, sdcClusterToken, sdcSlaveToken, runtimeInfo.getBaseHttpUrl(),
          authenticationToken.get(AuthzRole.ADMIN), authenticationToken.get(AuthzRole.CREATOR),
          authenticationToken.get(AuthzRole.MANAGER), authenticationToken.get(AuthzRole.GUEST), metrics);
      if (IS_TRACE_ENABLED) {
        LOG.trace("Calling back on " + callbackServerURL + " with the sdc url of " + runtimeInfo.getBaseHttpUrl());
      }
      Response response = ClientBuilder
        .newClient()
        .target(callbackServerURL)
        .request()
        .post(Entity.json(BeanHelper.wrapCallbackInfo(callbackInfo)));

      if (response.getStatus() != 200) {
        throw new RuntimeException("Failed : HTTP error code : "
          + response.getStatus());
      }

    } catch (Exception ex) {
      LOG.warn("Error while calling callback to Callback Server , {}", ex.getMessage(), ex);
    }
  }
}
