/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.callback.CallbackInfo;

public class CallbackInfoJson {

  private CallbackInfo callbackInfo;

  @JsonCreator
  public CallbackInfoJson(@JsonProperty("sdcClusterToken") String sdcClusterToken,
                          @JsonProperty("sdcSlaveToken") String sdcSlaveToken,
                          @JsonProperty("sdcURL") String sdcURL,
                          @JsonProperty("adminToken") String adminToken,
                          @JsonProperty("creatorToken") String creatorToken,
                          @JsonProperty("managerToken") String managerToken,
                          @JsonProperty("guestToken") String guestToken,
                          @JsonProperty("metrics") String metrics) {
    this.callbackInfo = new CallbackInfo(sdcClusterToken, sdcSlaveToken, sdcURL, adminToken, creatorToken, managerToken,
      guestToken, metrics);
  }


  public CallbackInfoJson(CallbackInfo callbackInfo) {
    this.callbackInfo = callbackInfo;
  }

  public String getSdcClusterToken() {
    return callbackInfo.getSdcClusterToken();
  }

  public String getSdcSlaveToken() {
    return callbackInfo.getSdcSlaveToken();
  }


  public String getSdcURL() {
    return callbackInfo.getSdcURL();
  }

  public String getAdminToken() {
    return callbackInfo.getAdminToken();
  }

  public String getCreatorToken() {
    return callbackInfo.getCreatorToken();
  }

  public String getManagerToken() {
    return callbackInfo.getManagerToken();
  }

  public String getGuestToken() {
    return callbackInfo.getGuestToken();
  }

  public String getMetrics() {
    return callbackInfo.getMetrics();
  }

  @JsonIgnore
  public CallbackInfo  getCallbackInfo() {
    return callbackInfo;
  }

}
