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
package com.streamsets.datacollector.callback;

import java.util.Objects;

public class CallbackInfo implements Comparable<CallbackInfo> {
  private final String sdcClusterToken;
  private final String sdcSlaveToken;
  private final String sdcURL;
  private final String adminToken;
  private final String creatorToken;
  private final String managerToken;
  private final String guestToken;
  private final CallbackObjectType callbackObjectType;
  private final String callbackObject;
  private final CallbackInfoHelper callbackInfoHelper;
  private final String name;
  private final String rev;
  private final String user;
  private final String slaveSdcId;

  public CallbackInfo(
      String user,
      String name,
      String rev,
      String sdcClusterToken,
      String sdcSlaveToken,
      String sdcURL,
      String adminToken,
      String creatorToken,
      String managerToken,
      String guestToken,
      CallbackObjectType callbackObjectType,
      String callbackObject,
      String slaveSdcId
  ) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.sdcClusterToken = sdcClusterToken;
    this.sdcSlaveToken = sdcSlaveToken;
    this.sdcURL = sdcURL;
    this.adminToken = adminToken;
    this.creatorToken = creatorToken;
    this.managerToken = managerToken;
    this.guestToken = guestToken;
    this.callbackObjectType = callbackObjectType;
    this.callbackObject = callbackObject;
    this.slaveSdcId = slaveSdcId;
    this.callbackInfoHelper = new CallbackInfoHelper(this);
  }

  public String getSdcClusterToken() {
    return sdcClusterToken;
  }

  public String getSdcSlaveToken() {
    return sdcSlaveToken;
  }

  public String getSdcURL() {
    return sdcURL;
  }

  public String getAdminToken() {
    return adminToken;
  }

  public String getManagerToken() {
    return managerToken;
  }

  public String getGuestToken() {
    return guestToken;
  }

  public String getCreatorToken() {
    return creatorToken;
  }

  public CallbackObjectType getCallbackObjectType() {
    return callbackObjectType;
  }

  public String getCallbackObject() {
    return callbackObject;
  }

  public CallbackInfoHelper getCallbackInfoHelper() {
    return callbackInfoHelper;
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return user;
  }

  public String getRev() {
    return rev;
  }

  public String getSlaveSdcId() {
    return slaveSdcId;
  }

  @Override
  public int compareTo(CallbackInfo o) {
    return sdcURL.compareToIgnoreCase(o.sdcURL);
  }

  @Override
  public boolean equals(Object o) {
    return compareTo((CallbackInfo)o) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sdcURL.toLowerCase());
  }

}
