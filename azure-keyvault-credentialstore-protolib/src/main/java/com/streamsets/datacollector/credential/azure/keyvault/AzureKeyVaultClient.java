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

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AzureKeyVaultClient extends KeyVaultCredentials {

  private String clientId;
  private String clientKey;
  private AuthenticationResult token;

  AzureKeyVaultClient(String clientId, String clientKey) {
    this.clientId = clientId;
    this.clientKey = clientKey;
  }

  @Override
  public String doAuthenticate(String authorization, String resource, String scope) {
    if (token == null || token.getExpiresOnDate().getTime() < now() - 60000){
      token = getAccessTokenFromClientCredentials(authorization, resource, clientId, clientKey);
    }
    return token.getAccessToken();
  }

  @VisibleForTesting
  AuthenticationResult getAccessTokenFromClientCredentials(
      String authorization, String resource, String clientId, String clientKey
  ) {
    AuthenticationContext context;
    AuthenticationResult result;
    ExecutorService service = null;
    try {
      service = Executors.newFixedThreadPool(1);
      context = new AuthenticationContext(authorization, false, service);
      ClientCredential credentials = new ClientCredential(clientId, clientKey);
      Future<AuthenticationResult> future = context.acquireToken(resource, credentials, null);
      result = future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (service != null) {
        service.shutdown();
      }
    }

    if (result == null) {
      throw new RuntimeException("authentication result was null");
    }
    return result;
  }

  @VisibleForTesting
  long now() {
    return System.currentTimeMillis();
  }
}
