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
package com.streamsets.datacollector.vault.api;

import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultConfiguration;

import java.util.Collections;

public class Authenticate extends VaultEndpoint {

  public Authenticate(VaultConfiguration conf, HttpTransport transport) throws VaultException {
    super(conf, transport);
  }

  public Secret appId(String appId, String userId) throws VaultException {
    HttpContent content = new JsonHttpContent(
        getJsonFactory(),
        ImmutableMap.of("app_id", appId, "user_id", userId)
    );
    return getSecret("/v1/auth/app-id/login", "POST", content);
  }

  public Secret appRole(String roleId, String secretId) throws VaultException {
    HttpContent content = new JsonHttpContent(
        getJsonFactory(),
        ImmutableMap.of("role_id", roleId, "secret_id", secretId)
    );
    return getSecret("/v1/auth/approle/login", "POST", content);
  }

  public Secret renewSelf() throws VaultException {
    HttpContent content = new JsonHttpContent(getJsonFactory(), Collections.emptyMap());
    return getSecret("/v1/auth/token/renew-self", "POST", content);
  }
}
