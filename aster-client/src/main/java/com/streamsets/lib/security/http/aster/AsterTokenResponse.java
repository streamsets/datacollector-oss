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
package com.streamsets.lib.security.http.aster;

/**
 * Bean that models the Ouath2 Aster token response.
 * <p/>
 * The property names are such that their JSON deserialization will map the OAuth2 parameters names.
 */
public class AsterTokenResponse {
  private String access_token;
  private int expires_in = Integer.MAX_VALUE;
  private String refresh_token;
  private long expires_on;

  /**
   * Returns the access token.
   */
  public String getAccess_token() {
    return access_token;
  }

  /**
   * Sets the access token.
   */
  public AsterTokenResponse setAccess_token(String access_token) {
    this.access_token = access_token;
    return this;
  }

  /**
   * Returns the access token expiration interval in seconds.
   */
  public int getExpires_in() {
    return expires_in;
  }

  /**
   * Sets the access token expiration interval in seconds.
   */
  public AsterTokenResponse setExpires_in(int expires_in) {
    this.expires_in = expires_in;
    return this;
  }

  /**
   * Returns the access token expiration date in Unix Epoch seconds.
   */
  public long getExpires_on() {
    return expires_on;
  }

  /**
   * Sets the access token expiration date in Unix Epoch seconds.
   */
  public AsterTokenResponse setExpires_on(long expires_on) {
    this.expires_on = expires_on;
    return this;
  }

  /**
   * Returns the refresh token.
   */
  public String getRefresh_token() {
    return refresh_token;
  }

  /**
   * Sets the refresh token.
   */
  public AsterTokenResponse setRefresh_token(String refresh_token) {
    this.refresh_token = refresh_token;
    return this;
  }
}
