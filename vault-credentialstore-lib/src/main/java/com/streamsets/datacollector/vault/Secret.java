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

public class Secret {
  @Key("lease_id")
  private String leaseId;
  @Key
  private boolean renewable;
  @Key("lease_duration")
  private int leaseDuration;
  @Key
  private Map<String, Object> data;
  @Key
  private SecretAuth auth;
  @Key
  private List<String> errors;

  public String getLeaseId() {
    return leaseId;
  }

  public boolean isRenewable() {
    return renewable;
  }

  public int getLeaseDuration() {
    return leaseDuration;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public SecretAuth getAuth() {
    return auth;
  }

  public List<String> getErrors() {
    return errors;
  }

  public Secret setLeaseId(String leaseId) {
    this.leaseId = leaseId;
    return this;
  }

  public Secret setRenewable(boolean renewable) {
    this.renewable = renewable;
    return this;
  }

  public Secret setLeaseDuration(int leaseDuration) {
    this.leaseDuration = leaseDuration;
    return this;
  }

  public Secret setData(Map<String, Object> data) {
    this.data = data;
    return this;
  }

  public Secret setAuth(SecretAuth auth) {
    this.auth = auth;
    return this;
  }

  public Secret setErrors(List<String> errors) {
    this.errors = errors;
    return this;
  }
}
