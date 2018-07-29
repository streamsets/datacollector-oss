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

import com.streamsets.datacollector.vault.Secret;
import com.streamsets.datacollector.vault.VaultClient;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.VaultConfigurationBuilder;
import com.streamsets.datacollector.vault.api.VaultTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;

public class TestLease extends VaultTestBase {

  @Test
  public void testRenew() throws Exception {
    VaultConfiguration conf = VaultConfigurationBuilder.newVaultConfiguration()
        .withAddress(address)
        .withToken(token)
        .build();

    String leaseId = "aws/creds/s3ReadOnly/895aad27-028e-301b-5475-906a6561f8f8";

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
        HttpRequest.request()
            .withMethod("PUT")
            .withHeader(tokenHeader)
            .withPath("/v1/sys/renew/" + leaseId),
        Times.exactly(1)
    )
    .respond(
        HttpResponse.response()
            .withStatusCode(HttpStatusCode.OK_200.code())
            .withHeader(contentJson)
            .withBody(getBody("sys_lease_renew.json"))
    );

    VaultClient vault = new VaultClient(conf);
    Secret secret = vault.sys().lease().renew(leaseId);
    Assert.assertEquals(leaseId, secret.getLeaseId());
    Assert.assertEquals(60, secret.getLeaseDuration());
    Assert.assertEquals(true, secret.isRenewable());
  }
}
