/*
 * Copyright 2019 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.credential.thycotic.api;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGetThycoticSecrets {

  @Test
  public void testGetSecretField() throws Exception {
    GetThycoticSecrets secrets = new GetThycoticSecrets();
    secrets = Mockito.spy(secrets);

    CloseableHttpClient closeableHttpClient = Mockito.mock(CloseableHttpClient.class);

    Mockito.doReturn("secret").when(secrets).getValue(closeableHttpClient, "u/api/v1/secrets/1/fields/p", "t");

    Assert.assertEquals("secret", secrets.getSecretField(closeableHttpClient, "t", "u", 1, "p", "g"));
  }

  @Test
  public void testNullSecretField() throws Exception {
    GetThycoticSecrets secrets = new GetThycoticSecrets();
    secrets = Mockito.spy(secrets);

    CloseableHttpClient closeableHttpClient = Mockito.mock(CloseableHttpClient.class);

    Mockito.doReturn(null).when(secrets).getValue(closeableHttpClient, "u/api/v1/secrets/1/fields/p", "t");

    Assert.assertEquals(null, secrets.getSecretField(closeableHttpClient, "t", "u", 1, "p", "g"));
  }

}
