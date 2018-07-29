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
package com.streamsets.datacollector.vault;

import com.google.api.client.util.Key;

import java.util.List;
import java.util.Map;

public class SecretAuth {
  @Key("client_token")
  private String clientToken;
  @Key
  private String accessor;
  @Key
  private List<String> policies;
  @Key
  private Map<String, String> metadata;
  @Key("lease_duration")
  private int leaseDuration;
  @Key
  private boolean renewable;

  public String getClientToken() {
    return clientToken;
  }

  public SecretAuth setClientToken(String clientToken) {
    this.clientToken = clientToken;
    return this;
  }

  public String getAccessor() {
    return accessor;
  }

  public SecretAuth setAccessor(String accessor) {
    this.accessor = accessor;
    return this;
  }

  public List<String> getPolicies() {
    return policies;
  }

  public SecretAuth setPolicies(List<String> policies) {
    this.policies = policies;
    return this;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public SecretAuth setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
    return this;
  }

  public int getLeaseDuration() {
    return leaseDuration;
  }

  public SecretAuth setLeaseDuration(int leaseDuration) {
    this.leaseDuration = leaseDuration;
    return this;
  }

  public boolean isRenewable() {
    return renewable;
  }

  public SecretAuth setRenewable(boolean renewable) {
    this.renewable = renewable;
    return this;
  }
}
