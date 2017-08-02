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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultClient;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.VaultConfigurationBuilder;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.NO_CONTENT_204;
import static org.mockserver.model.HttpStatusCode.OK_200;

public class TestLogical extends VaultTestBase {

  @Test
  public void testRead() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(address)
        .withToken(token)
        .build();

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withMethod("GET")
                .withHeader(tokenHeader)
                .withPath("/v1/secret/streamsets"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(getBody("logical_read.json"))
        );

    VaultClient vault = new VaultClient(conf);
    Secret secret = vault.logical().read("secret/streamsets");

    assertEquals(2592000, secret.getLeaseDuration());
    assertEquals("", secret.getLeaseId());
    assertEquals(false, secret.isRenewable());
    assertTrue(secret.getData().containsKey("ingest"));
    assertEquals("is fun", secret.getData().get("ingest"));
  }

  @Test
  public void testWrite() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(address)
        .withToken(token)
        .build();

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withMethod("POST")
                .withHeader(tokenHeader)
                .withPath("/v1/secret/hello")
                .withBody("{\"value\":\"world!\"}"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(NO_CONTENT_204.code())
                .withHeader(contentJson)
        );

    VaultClient vault = new VaultClient(conf);
    vault.logical().write("secret/hello", ImmutableMap.<String, Object>of("value", "world!"));
  }
}
