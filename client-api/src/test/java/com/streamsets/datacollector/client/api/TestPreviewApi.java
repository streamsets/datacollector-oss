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
import com.streamsets.datacollector.client.model.PreviewInfoJson;
import com.streamsets.datacollector.client.model.StageOutputJson;
import com.streamsets.datacollector.client.util.TestUtil;
import com.streamsets.datacollector.task.Task;
import com.streamsets.testing.NetworkUtils;
import com.streamsets.testing.ParametrizedUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class TestPreviewApi {

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

  public TestPreviewApi(String authType) {
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

      testValidationConfig(apiClient);
      testRunningPreview(apiClient);

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

  private void testValidationConfig(ApiClient apiClient ) throws ApiException, InterruptedException {
    StoreApi storeApi = new StoreApi(apiClient);
    PreviewApi previewApi = new PreviewApi(apiClient);

    String pipelineName = "testValidationConfig";

    //Create Pipeline
    PipelineConfigurationJson pipelineConfig = storeApi.createPipeline(
        pipelineName,
      "Testing getPipeline test case",
        false
    );
    Assert.assertNotNull(pipelineConfig);

    PreviewInfoJson previewInfoJson = previewApi.validateConfigs(pipelineName, "0", 5000L);
    Assert.assertNotNull(pipelineConfig);
    Assert.assertNotNull(previewInfoJson.getPreviewerId());
    //Assert.assertEquals(PreviewInfoJson.StatusEnum.VALIDATING, previewInfoJson.getStatus());

    PreviewInfoJson lastPreviewStatus;
    while(true) {
      PreviewInfoJson previewInfo = previewApi.getPreviewStatus(pipelineName, previewInfoJson.getPreviewerId());
      if(!previewInfo.getStatus().isOneOf(PreviewInfoJson.StatusEnum.VALIDATING)) {
        lastPreviewStatus = previewInfo;
        break;
      }

      Thread.sleep(500L);
    }

    Assert.assertNotNull(lastPreviewStatus);
    Assert.assertEquals(PreviewInfoJson.StatusEnum.VALIDATION_ERROR, lastPreviewStatus.getStatus());
  }

  private void testRunningPreview(ApiClient apiClient ) throws ApiException, InterruptedException {
    StoreApi storeApi = new StoreApi(apiClient);
    PreviewApi previewApi = new PreviewApi(apiClient);

    String pipelineName = "testRunningPreview";

    //Create Pipeline
    PipelineConfigurationJson pipelineConfig = storeApi.createPipeline(
        pipelineName,
      "Testing getPipeline test case",
        false
    );
    Assert.assertNotNull(pipelineConfig);

    PreviewInfoJson previewInfoJson = previewApi.previewWithOverride(pipelineName,
      Collections.<StageOutputJson>emptyList(), "0", 10, 1, false, null, null);
    Assert.assertNotNull(pipelineConfig);
    Assert.assertNotNull(previewInfoJson.getPreviewerId());

    if(previewInfoJson.getStatus() != null) {
      Assert.assertTrue(
        "Unexpected status: " + previewInfoJson.getStatus().name(),
        previewInfoJson.getStatus().isOneOf(
          PreviewInfoJson.StatusEnum.CREATED,
          PreviewInfoJson.StatusEnum.STARTING,
          PreviewInfoJson.StatusEnum.RUNNING,
          PreviewInfoJson.StatusEnum.RUN_ERROR
        )
      );
    }

    PreviewInfoJson lastPreviewStatus;
    while(true) {
      PreviewInfoJson previewInfo = previewApi.getPreviewStatus(pipelineName, previewInfoJson.getPreviewerId());
      if(previewInfo.getStatus().isOneOf(PreviewInfoJson.StatusEnum.RUN_ERROR)) {
        lastPreviewStatus = previewInfo;
        break;
      }

      Thread.sleep(500L);
    }

    Assert.assertNotNull(lastPreviewStatus);
    Assert.assertEquals(PreviewInfoJson.StatusEnum.RUN_ERROR, lastPreviewStatus.getStatus());
  }
}
