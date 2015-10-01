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
import com.streamsets.datacollector.client.model.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.RuleDefinitionsJson;
import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.task.Task;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestStoreApi {
  private String baseURL;
  private String[] authenticationTypes = {"none", "basic", "form", "digest"};

  @Test
  public void testForDifferentAuthenticationTypes() {
    try {
      for(String authType: authenticationTypes) {
        int port = TestUtil.getRandomPort();
        Task server = TestUtil.startServer(port, authType);
        baseURL = "http://127.0.0.1:" + port;
        ApiClient apiClient = getApiClient(authType);
        StoreApi storeApi = new StoreApi(apiClient);
        testStoreAPI(storeApi);

        TestUtil.stopServer(server);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private ApiClient getApiClient(String authenticationType) {
    ApiClient apiClient = new ApiClient(authenticationType);
    apiClient.setBasePath(baseURL+ "/rest");
    apiClient.setUsername("admin");
    apiClient.setPassword("admin");
    return apiClient;
  }

  private void testStoreAPI(StoreApi storeApi) throws ApiException {
    String pipelineName = "testGetPipeline";

    //Create Pipeline
    PipelineConfigurationJson pipelineConfig = storeApi.createPipeline(pipelineName,
      "Testing getPipeline test case");
    Assert.assertNotNull(pipelineConfig);

    //Fetch Pipeline Configuration
    pipelineConfig = storeApi.getPipelineInfo(pipelineName, "0", "pipeline", false);
    Assert.assertNotNull(pipelineConfig);

    //Update Pipeline Configuration
    PipelineConfigurationJson updatedPipelineConfig = storeApi.savePipeline(pipelineName, pipelineConfig, "0",
      "Change description");
    Assert.assertNotNull(updatedPipelineConfig);
    Assert.assertNotEquals(pipelineConfig.getInfo().getUuid(), updatedPipelineConfig.getInfo().getUuid());


    //Fetch Pipeline Rules
    RuleDefinitionsJson pipelineRules = storeApi.getPipelineRules(pipelineName, "0");
    Assert.assertNotNull(pipelineRules);
    List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsonList = pipelineRules.getMetricsRuleDefinitions();
    Assert.assertNotNull(metricsRuleDefinitionJsonList);
    Assert.assertTrue(metricsRuleDefinitionJsonList.size() > 0);

    //Update Pipeline Rules
    MetricsRuleDefinitionJson metricRule = metricsRuleDefinitionJsonList.get(0);
    metricRule.setEnabled(true);
    metricsRuleDefinitionJsonList.remove(2);
    RuleDefinitionsJson updatedPipelineRules = storeApi.savePipelineRules(pipelineName, pipelineRules, "0");
    Assert.assertNotNull(updatedPipelineRules);
    Assert.assertNotEquals(pipelineRules.getUuid(), updatedPipelineRules.getUuid());
  }


}
