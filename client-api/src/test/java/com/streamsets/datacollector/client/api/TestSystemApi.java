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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.task.Task;
import com.streamsets.testing.NetworkUtils;
import com.streamsets.testing.ParametrizedUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestSystemApi {

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

  public TestSystemApi(String authType) {
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

      SystemApi systemApi = new SystemApi(apiClient);

      testGetConfiguration(systemApi);
      testGetSDCDirectories(systemApi);
      testGetBuildInfo(systemApi);
      testGetUserInfo(systemApi);
      testGetServerTime(systemApi);
      testGetStats(systemApi);
      testGetThreadsDump(systemApi);

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

  public void testGetConfiguration(SystemApi systemApi) throws ApiException  {
    Map<String, Object> configuration = systemApi.getConfiguration();
    Assert.assertNotNull(configuration);
    Assert.assertNotNull(configuration.get(WebServerTask.HTTP_PORT_KEY));
    Assert.assertNotNull(configuration.get(WebServerTask.AUTHENTICATION_KEY));
  }

  public void testGetSDCDirectories(SystemApi systemApi) throws ApiException  {
    Map<String, Object> sdcDirectories = systemApi.getSDCDirectories();
    Assert.assertNotNull(sdcDirectories);
    Assert.assertNotNull(sdcDirectories.get("runtimeDir"));
    Assert.assertNotNull(sdcDirectories.get("configDir"));
    Assert.assertNotNull(sdcDirectories.get("dataDir"));
    Assert.assertNotNull(sdcDirectories.get("logDir"));
    Assert.assertNotNull(sdcDirectories.get("resourcesDir"));
  }

  public void testGetBuildInfo(SystemApi systemApi) throws ApiException {
    Map<String, Object> buildInfo = systemApi.getBuildInfo();
    Assert.assertNotNull(buildInfo);
    Assert.assertNotNull(buildInfo.get("builtDate"));
    Assert.assertNotNull(buildInfo.get("builtBy"));
    Assert.assertNotNull(buildInfo.get("version"));
  }

  public void testGetUserInfo(SystemApi systemApi) throws ApiException  {
    Map<String, Object> userInfo = systemApi.getUserInfo();
    Assert.assertNotNull(userInfo);
    Assert.assertNotNull(userInfo.get("roles"));
    Assert.assertNotNull(userInfo.get("user"));
    Assert.assertEquals(userInfo.get("user"), "admin");
  }

  public void testGetServerTime(SystemApi systemApi) throws ApiException  {
    Map<String, Object> serverTime = systemApi.getServerTime();
    Assert.assertNotNull(serverTime);
    Assert.assertNotNull(serverTime.get("serverTime"));
  }

  public void testGetStats(SystemApi systemApi) throws ApiException {
    Map<String, Object> stats = systemApi.getStats();
    Assert.assertNotNull(stats);
    Assert.assertEquals(ImmutableMap.of(
        "opted", false,
        "active", false
    ), stats);
  }

  public void testGetThreadsDump(SystemApi systemApi) throws ApiException  {
    List<Map<String, Object>> threadsDump = systemApi.getThreadsDump();
    Assert.assertNotNull(threadsDump);
    Assert.assertTrue(threadsDump.size() > 0);
  }

}
