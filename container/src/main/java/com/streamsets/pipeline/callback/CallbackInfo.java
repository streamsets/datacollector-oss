/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.callback;

public class CallbackInfo {
  private final String sdcClusterToken;
  private final String sdcURL;
  private final String adminToken;
  private final String creatorToken;
  private final String managerToken;
  private final String guestToken;

  public CallbackInfo(String sdcClusterToken, String sdcURL, String adminToken, String creatorToken,
                      String managerToken, String guestToken) {
    this.sdcClusterToken = sdcClusterToken;
    this.sdcURL = sdcURL;
    this.adminToken = adminToken;
    this.creatorToken = creatorToken;
    this.managerToken = managerToken;
    this.guestToken = guestToken;
  }

  public String getSdcClusterToken() {
    return sdcClusterToken;
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
}
