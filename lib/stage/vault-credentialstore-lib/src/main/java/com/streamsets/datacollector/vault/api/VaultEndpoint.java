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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class VaultEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(VaultEndpoint.class);
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private final HttpRequestFactory requestFactory;
  private final VaultConfiguration conf;

  public VaultEndpoint(final VaultConfiguration conf, HttpTransport transport) throws VaultException {
    this.conf = conf;
    requestFactory = transport.createRequestFactory(
        new HttpRequestInitializer() {
          @Override
          public void initialize(HttpRequest request) throws IOException {
            request.setParser(new JsonObjectParser(JSON_FACTORY));
            request.setHeaders(new HttpHeaders().set("X-Vault-Token", conf.getToken()));
            request.setReadTimeout(conf.getReadTimeout());
            request.setConnectTimeout(conf.getOpenTimeout());
          }
        }
    );
  }

  protected HttpRequestFactory getRequestFactory() {
    return requestFactory;
  }

  protected static JsonFactory getJsonFactory() {
    return JSON_FACTORY;
  }

  protected VaultConfiguration getConf() {
    return conf;
  }

  protected Secret getSecret(String path, String method, HttpContent content) throws VaultException {
    try {
      HttpRequest request = getRequestFactory().buildRequest(
          method,
          new GenericUrl(getConf().getAddress() + path),
          content
      );
      HttpResponse response = request.execute();
      if (!response.isSuccessStatusCode()) {
        LOG.error("Request failed status: {} message: {}", response.getStatusCode(), response.getStatusMessage());
      }

      return response.parseAs(Secret.class);
    } catch (IOException e) {
      LOG.error(e.toString(), e);
      throw new VaultException("Failed to authenticate: " + e.toString(), e);
    }
  }

  protected Secret getSecret(String path, String method) throws VaultException {
    return getSecret(path, method, null);
  }
}
