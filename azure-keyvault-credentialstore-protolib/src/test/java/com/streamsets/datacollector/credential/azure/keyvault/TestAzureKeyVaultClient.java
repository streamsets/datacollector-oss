/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.credential.azure.keyvault;

import com.microsoft.aad.adal4j.AuthenticationResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Calendar;
import java.util.Date;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AuthenticationResult.class})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestAzureKeyVaultClient {

  private static final String scope = "Scope";
  private static final String authorization = "auth";
  private static final String resource = "resource";

  private static final String clientID = "clientID";
  private static final String clientKey = "clientKey";

  @Test
  public void doAuthenticate() {
    AzureKeyVaultClient client = new AzureKeyVaultClient(clientID, clientKey);
    client = Mockito.spy(client);

    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date());
    Date date = cal.getTime();

    AuthenticationResult authenticationResult = PowerMockito.mock(AuthenticationResult.class);

    Mockito.when(authenticationResult.getExpiresOnDate()).thenReturn(date);
    Mockito.when(authenticationResult.getAccessToken()).thenReturn("I'mAToken");

    Mockito.doReturn(authenticationResult).when(client).getAccessTokenFromClientCredentials(Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any()
    );

    String token = client.doAuthenticate(authorization, scope, resource);

    //Verify that is not being executed the second time, it should get the one from cache
    Mockito.doReturn(null).when(client).getAccessTokenFromClientCredentials(Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any()
    );
    Mockito.when(authenticationResult.getAccessToken()).thenReturn("I'mAToken1");

    String token1 = client.doAuthenticate(authorization, scope, resource);

    cal.add(Calendar.HOUR, +8);
    Date eightHoursAgo = cal.getTime();

    Mockito.doReturn(eightHoursAgo.getTime()).when(client).now();

    String token2;
    try {
      token2 = client.doAuthenticate(authorization, scope, resource);
      Assert.fail();
    } catch (NullPointerException e) {
      Mockito.doReturn(authenticationResult).when(client).getAccessTokenFromClientCredentials(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
      Mockito.when(authenticationResult.getAccessToken()).thenReturn("I'mAToken2");
      token2 = client.doAuthenticate(authorization, scope, resource);
    }

    Assert.assertNotEquals(token1, token);
    Assert.assertEquals("I'mAToken", token);
    Assert.assertEquals("I'mAToken1", token1);
    Assert.assertEquals("I'mAToken2", token2);
  }
}
