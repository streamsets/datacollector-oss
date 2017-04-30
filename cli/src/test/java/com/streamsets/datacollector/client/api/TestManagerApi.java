/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.junit.Assert;
import org.junit.Test;

public class TestManagerApi {
  private String baseURL;
  private String[] authenticationTypes = {"none", "basic", "form", "digest"};

  @Test
  public void testForDifferentAuthenticationTypes() {
    Task server = null;
    try {
      for(String authType: authenticationTypes) {
        int port = NetworkUtils.getRandomPort();
        server = TestUtil.startServer(port, authType);
        baseURL = "http://127.0.0.1:" + port;
        ApiClient apiClient = getApiClient(authType);
        testManagerAPI(apiClient);

        TestUtil.stopServer(server);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if(server != null) {
        TestUtil.stopServer(server);
      }
    }
  }

  private ApiClient getApiClient(String authenticationType) {
    ApiClient apiClient = new ApiClient(authenticationType);
    apiClient.setBasePath(baseURL+ "/rest");
    apiClient.setUsername("admin");
    apiClient.setPassword("admin");
    return apiClient;
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
    pipelineState = managerApi.startPipeline(pipelineName, "0", null);
    Assert.assertEquals(PipelineStateJson.StatusEnum.STARTING, pipelineState.getStatus());

    Thread.sleep(500L);

    //Get Pipeline Status
    pipelineState = managerApi.getPipelineStatus(pipelineName, "0");
    Assert.assertEquals(PipelineStateJson.StatusEnum.START_ERROR, pipelineState.getStatus());

  }


}
