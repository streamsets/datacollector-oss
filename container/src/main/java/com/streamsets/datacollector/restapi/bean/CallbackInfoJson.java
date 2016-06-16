/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.callback.CallbackInfo;

public class CallbackInfoJson {

  private final CallbackInfo callbackInfo;

  @JsonCreator
  public CallbackInfoJson(
                          @JsonProperty("user") String user,
                          @JsonProperty("name") String name,
                          @JsonProperty("rev") String rev,
                          @JsonProperty("sdcClusterToken") String sdcClusterToken,
                          @JsonProperty("sdcSlaveToken") String sdcSlaveToken,
                          @JsonProperty("sdcURL") String sdcURL,
                          @JsonProperty("adminToken") String adminToken,
                          @JsonProperty("creatorToken") String creatorToken,
                          @JsonProperty("managerToken") String managerToken,
                          @JsonProperty("guestToken") String guestToken,
                          @JsonProperty("metrics") String metrics,
                          @JsonProperty("slaveSdcId") String slaveSdcId) {
    this.callbackInfo = new CallbackInfo(user, name, rev, sdcClusterToken, sdcSlaveToken, sdcURL,
      adminToken, creatorToken, managerToken, guestToken, metrics, slaveSdcId);
  }


  public CallbackInfoJson(CallbackInfo callbackInfo) {
    this.callbackInfo = callbackInfo;
  }

  public String getUser() {
    return callbackInfo.getUser();
  }

  public String getName() {
    return callbackInfo.getName();
  }

  public String getRev() {
    return callbackInfo.getRev();
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

  public String getSlaveSdcId() {
    return callbackInfo.getSlaveSdcId();
  }

  @JsonIgnore
  public CallbackInfo  getCallbackInfo() {
    return callbackInfo;
  }

}
