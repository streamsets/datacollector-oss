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

import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultClient;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.VaultConfigurationBuilder;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.OK_200;

public class TestAuthenticate extends VaultTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuthenticate.class);

  @Test
  public void testAppId() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(address)
        .withToken(token)
        .build();

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
        request()
            .withMethod("POST")
            .withHeader(tokenHeader)
            .withPath("/v1/auth/app-id/login")
            .withBody("{\"app_id\":\"foo\",\"user_id\":\"bar\"}"),
        exactly(1)
    )
    .respond(
        response()
            .withStatusCode(OK_200.code())
            .withHeader(contentJson)
            .withBody(getBody("auth_app_id.json"))
    );

    VaultClient vault = new VaultClient(conf);
    Secret secret = vault.authenticate().appId("foo", "bar");

    assertEquals("26845a04-96f4-2b21-e404-c6e858d08a71", secret.getAuth().getClientToken());
    assertEquals("sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33", secret.getAuth().getMetadata().get("app-id"));
    assertEquals("sha1:62cdb7020ff920e5aa642c3d4066950dd1f01f4d", secret.getAuth().getMetadata().get("user-id"));
    assertEquals(0, secret.getLeaseDuration());
    assertEquals("", secret.getLeaseId());
    assertEquals(false, secret.isRenewable());
  }
}
