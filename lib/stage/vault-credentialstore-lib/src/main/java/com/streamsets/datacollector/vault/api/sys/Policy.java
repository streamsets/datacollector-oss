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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.api.VaultEndpoint;
import com.streamsets.datacollector.vault.api.VaultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Policy extends VaultEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(Policy.class);

  public Policy(VaultConfiguration conf, HttpTransport transport) throws VaultException {
    super(conf, transport);
  }

  public boolean create(String name, String rules) throws VaultException {
    Map<String, Object> data = new HashMap<>();
    data.put("rules", rules);

    HttpContent content = new JsonHttpContent(getJsonFactory(), data);

    try {
      HttpRequest request = getRequestFactory().buildRequest(
          "POST",
          new GenericUrl(getConf().getAddress() + "/v1/sys/policy/" + name),
          content
      );
      HttpResponse response = request.execute();
      if (!response.isSuccessStatusCode()) {
        LOG.error("Request failed status: {} message: {}", response.getStatusCode(), response.getStatusMessage());
      }

      return response.isSuccessStatusCode();
    } catch (IOException e) {
      LOG.error(e.toString(), e);
      throw new VaultException("Failed to authenticate: " + e.toString(), e);
    }
  }
}
