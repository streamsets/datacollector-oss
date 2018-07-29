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

import com.streamsets.datacollector.vault.VaultClient;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.VaultConfigurationBuilder;
import com.streamsets.datacollector.vault.api.VaultTestBase;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;

import static org.junit.Assert.assertTrue;

public class TestAuth extends VaultTestBase {

  @Test
  public void testEnableAuth() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(address)
        .withToken(token)
        .build();

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
        HttpRequest.request()
            .withMethod("POST")
            .withHeader(tokenHeader)
            .withPath("/v1/sys/auth/app-id")
            .withBody("{\"type\":\"app-id\"}"),
        Times.exactly(1)
    )
    .respond(
        HttpResponse.response()
            .withStatusCode(HttpStatusCode.NO_CONTENT_204.code())
            .withHeader(contentJson)
    );

    VaultClient vault = new VaultClient(conf);
    assertTrue(vault.sys().auth().enable("app-id", "app-id"));
  }
}
