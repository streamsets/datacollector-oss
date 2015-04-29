/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.callback;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.util.AuthzRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.Map;

public class CallbackServerRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(CallbackServerRunnable.class);
  public static final String RUNNABLE_NAME = "CallbackServerRunnable";
  public static final String SDC_CLUSTER_TOKEN = "sdcClusterToken";

  private final RuntimeInfo runtimeInfo;
  private final String callbackServerURL;
  private final String sdcClusterToken;

  public CallbackServerRunnable(RuntimeInfo runtimeInfo, String callbackServerURL, String sdcClusterToken) {
    this.runtimeInfo = runtimeInfo;
    this.callbackServerURL = callbackServerURL;
    this.sdcClusterToken = sdcClusterToken;
  }

  @Override
  public void run() {
    String originalName = Thread.currentThread().getName();
    Thread.currentThread().setName(originalName + "-" + RUNNABLE_NAME);
    try {
      Map<String, String> authenticationToken = runtimeInfo.getAuthenticationTokens();
      CallbackInfo callbackInfo = new CallbackInfo(sdcClusterToken,
        runtimeInfo.getBaseHttpUrl(),
        authenticationToken.get(AuthzRole.ADMIN),
        authenticationToken.get(AuthzRole.CREATOR),
        authenticationToken.get(AuthzRole.MANAGER),
        authenticationToken.get(AuthzRole.GUEST));

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
    } finally {
      Thread.currentThread().setName(originalName);
    }
  }
}
