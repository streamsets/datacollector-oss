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
package com.streamsets.datacollector.vault.api.sys;

import com.google.api.client.http.HttpTransport;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.api.VaultException;

public class Sys {
  private final Lease lease;
  private final Auth auth;
  private final Mounts mounts;
  private final Policy policy;

  public Sys(VaultConfiguration conf, HttpTransport httpTransport) throws VaultException {
    this.lease = new Lease(conf, httpTransport);
    this.auth = new Auth(conf, httpTransport);
    this.mounts = new Mounts(conf, httpTransport);
    this.policy = new Policy(conf, httpTransport);
  }

  public Lease lease() {
    return lease;
  }
  public Auth auth() {
    return auth;
  }
  public Mounts mounts() {
    return mounts;
  }
  public Policy policy() {
    return policy;
  }
}
