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
import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultConfiguration;

import java.util.Map;

public class Logical extends VaultEndpoint {

  public Logical(VaultConfiguration conf, HttpTransport transport) throws VaultException {
    super(conf, transport);
  }

  public Secret read(String path) throws VaultException {
    return getSecret("/v1/" + path, "GET");
  }

  public Secret write(String path, Map<String, Object> params) throws VaultException {
    HttpContent content = new JsonHttpContent(
        getJsonFactory(),
        params
    );
    return getSecret("/v1/" + path, "POST", content);
  }
}
