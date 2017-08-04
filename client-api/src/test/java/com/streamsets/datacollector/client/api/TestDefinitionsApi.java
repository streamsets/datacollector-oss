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
package com.streamsets.datacollector.client.api;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.model.DefinitionsJson;
import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.task.Task;
import com.streamsets.testing.NetworkUtils;
import com.streamsets.testing.ParametrizedUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

@RunWith(Parameterized.class)
public class TestDefinitionsApi {

  @Parameterized.Parameters(name = "str({0})")
  public static Collection<Object[]> data() throws Exception {
    return ParametrizedUtils.toArrayOfArrays(
      "none",
      "basic",
      "form",
      "digest"
    );
  }

  private String baseURL;
  private String authType;

  public TestDefinitionsApi(String authType) {
    this.authType = authType;
  }

  @Test
  public void testForDifferentAuthenticationTypes() throws Exception {
    Task server = null;
    try {
      int port = NetworkUtils.getRandomPort();
      server = TestUtil.startServer(port, authType);
      baseURL = "http://127.0.0.1:" + port;
      ApiClient apiClient = getApiClient(authType);

      DefinitionsApi definitionsApi = new DefinitionsApi(apiClient);

      testGetDefinitions(definitionsApi);

      if(!authType.equals("none")) {
        testInvalidUserNamePassword(authType);
      }

      TestUtil.stopServer(server);
    } finally {
      if(server != null) {
        TestUtil.stopServer(server);
      }
    }
  }

  private ApiClient getApiClient(String authenticationType) {
    return new ApiClient(authenticationType)
        .setBasePath(baseURL + "/rest")
        .setUsername("admin")
        .setPassword("admin");
  }

  public void testGetDefinitions(DefinitionsApi definitionsApi) throws ApiException  {
    DefinitionsJson definitions = definitionsApi.getDefinitions();
    Assert.assertNotNull(definitions);
  }

  public void testInvalidUserNamePassword(String authType) {
    ApiClient apiClient = new ApiClient(authType);
    apiClient.setBasePath(baseURL + "/rest");
    apiClient.setUsername("notvaliduser");
    apiClient.setPassword("notvalidpassword");

    boolean exceptionThrown = false;
    DefinitionsApi definitionsApi = new DefinitionsApi(apiClient);
    try {
      definitionsApi.getDefinitions();
    } catch (ApiException e) {
      exceptionThrown = true;
      Assert.assertEquals("HTTP Error 401 - Unauthorized: Access is denied due to invalid credentials.",
        e.getLocalizedMessage());
    }

    Assert.assertTrue(exceptionThrown);
  }
}
