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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.ApiException;
import com.streamsets.datacollector.client.model.MetricsRuleDefinitionJson;
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.PipelineFragmentEnvelopeJson;
import com.streamsets.datacollector.client.model.RuleDefinitionsJson;
import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.task.Task;
import com.streamsets.testing.NetworkUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestStoreApi {
  private String baseURL;

  @Parameters(name = "authType:{0}")
  public static Collection<String> authTypes() {
    return ImmutableList.of("none", "basic", "form", "digest");
  }

  private String authType;

  public TestStoreApi(String authType) {
    this.authType = authType;
  }

  @Test
  public void testForDifferentAuthenticationTypes() {
    Task server = null;
    try {
      int port = NetworkUtils.getRandomPort();
      server = TestUtil.startServer(port, authType);
      baseURL = "http://127.0.0.1:" + port;
      ApiClient apiClient = getApiClient(authType);
      StoreApi storeApi = new StoreApi(apiClient);

      testStoreAPI(storeApi);
      testStoreAPINegativeCases(storeApi);
      testPipelineFragmentAPI(storeApi);

      TestUtil.stopServer(server);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
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

  private void testStoreAPI(StoreApi storeApi) throws ApiException {
    String pipelineName = "testGetPipeline";

    // Create Pipeline
    PipelineConfigurationJson pipelineConfig = storeApi.createPipeline(
        pipelineName,
      "Testing getPipeline test case",
        false
    );
    Assert.assertNotNull(pipelineConfig);

    // Fetch Pipeline Configuration
    pipelineConfig = storeApi.getPipelineInfo(pipelineName, "0", "pipeline", false);
    Assert.assertNotNull(pipelineConfig);

    // Update Pipeline Configuration
    PipelineConfigurationJson updatedPipelineConfig = storeApi.savePipeline(pipelineName, pipelineConfig, "0",
      "Change description");
    Assert.assertNotNull(updatedPipelineConfig);
    Assert.assertNotEquals(pipelineConfig.getInfo().getUuid(), updatedPipelineConfig.getInfo().getUuid());


    // Fetch Pipeline Rules
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

  private void testPipelineFragmentAPI(StoreApi storeApi) throws ApiException {
    String pipelineName = "testGetPipelineFragment";

    // Create Pipeline Fragment
    PipelineFragmentEnvelopeJson fragmentEnvelope = storeApi.createDraftPipelineFragment(
        pipelineName,
        "Testing Fragment",
        null
    );
    Assert.assertNotNull(fragmentEnvelope);

    // Update Pipeline Fragment Configuration
    PipelineFragmentEnvelopeJson updatedFragmentEnvelope = storeApi.importPipelineFragment(
        fragmentEnvelope.getPipelineFragmentConfig().getFragmentId(),
        true,
        false,
        fragmentEnvelope
    );
    Assert.assertNotNull(updatedFragmentEnvelope);
  }

  private void testStoreAPINegativeCases(StoreApi storeApi) {
    //Fetch Invalid Pipeline Configuration
    boolean exceptionTriggered = false;
    try {
      PipelineConfigurationJson pipelineConfig = storeApi.getPipelineInfo("Not a valid pipeline", "0", "pipeline", false);
      Assert.assertNotNull(pipelineConfig);
    } catch (ApiException ex) {
      exceptionTriggered = true;
      Assert.assertEquals(ex.getCode(), 500);
      Assert.assertTrue(ex.getMessage().contains("CONTAINER_0200 - Pipeline 'Not a valid pipeline' does not exist"));
    }
    Assert.assertTrue(exceptionTriggered);
  }
}
