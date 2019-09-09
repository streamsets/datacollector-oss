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
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
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
public class TestManagerApi {

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

  public TestManagerApi(String authType) {
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
      testManagerAPI(apiClient);

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

  private void testManagerAPI(ApiClient apiClient) throws ApiException, InterruptedException {
    StoreApi storeApi = new StoreApi(apiClient);
    ManagerApi managerApi = new ManagerApi(apiClient);

    String pipelineName = "testManagerAPI";

    //Create Pipeline
    PipelineConfigurationJson pipelineConfig = storeApi.createPipeline(
        pipelineName,
      "Testing getPipeline test case",
        false
    );
    Assert.assertNotNull(pipelineConfig);

    //Get Pipeline Status
    PipelineStateJson pipelineState = managerApi.getPipelineStatus(pipelineName, "0");
    Assert.assertEquals(PipelineStateJson.StatusEnum.EDITED, pipelineState.getStatus());

    //Try to start invalid pipeline
    managerApi.startPipeline(pipelineName, "0", null);

    Thread.sleep(500L);

    //Get Pipeline Status
    pipelineState = managerApi.getPipelineStatus(pipelineName, "0");
    Assert.assertEquals(PipelineStateJson.StatusEnum.START_ERROR, pipelineState.getStatus());

  }


}
