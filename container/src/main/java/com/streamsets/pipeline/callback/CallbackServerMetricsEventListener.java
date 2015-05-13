/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.callback;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.util.AuthzRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.Map;


public class CallbackServerMetricsEventListener implements MetricsEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackServerMetricsEventListener.class);
  private final RuntimeInfo runtimeInfo;
  private final String callbackServerURL;
  private final String sdcClusterToken;

  public CallbackServerMetricsEventListener(RuntimeInfo runtimeInfo, String callbackServerURL, String sdcClusterToken) {
    this.runtimeInfo = runtimeInfo;
    this.callbackServerURL = callbackServerURL;
    this.sdcClusterToken = sdcClusterToken;
  }

  @Override
  public void notification(String metrics) {
    try {
      // this is in the slave, as such getSDCToken returns the slave token
      String sdcSlaveToken = Utils.checkNotNull(runtimeInfo.getSDCToken(), "SDC Slave Token");
      Map<String, String> authenticationToken = runtimeInfo.getAuthenticationTokens();
      CallbackInfo callbackInfo = new CallbackInfo(sdcClusterToken,
        sdcSlaveToken,
        runtimeInfo.getBaseHttpUrl(),
        authenticationToken.get(AuthzRole.ADMIN),
        authenticationToken.get(AuthzRole.CREATOR),
        authenticationToken.get(AuthzRole.MANAGER),
        authenticationToken.get(AuthzRole.GUEST),
        metrics);

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
