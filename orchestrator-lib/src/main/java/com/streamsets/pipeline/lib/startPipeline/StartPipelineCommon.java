/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.startPipeline;

import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class StartPipelineCommon {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineCommon.class);
  public static final String SSL_CONFIG_PREFIX = "conf.tlsConfig.";
  public ManagerApi managerApi;
  public StoreApi storeApi;
  private StartPipelineConfig conf;

  public StartPipelineCommon(StartPipelineConfig conf) {
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(List<Stage.ConfigIssue> issues, Stage.Context context) {
    if (conf.tlsConfig.isEnabled()) {
      conf.tlsConfig.init(
          context,
          Groups.TLS.name(),
          SSL_CONFIG_PREFIX,
          issues
      );
    }

    if (CollectionUtils.isNotEmpty(conf.pipelineIdConfigList)) {
      int index = 1;
      for (PipelineIdConfig pipelineIdConfig: conf.pipelineIdConfigList) {
        if (StringUtils.isEmpty(pipelineIdConfig.pipelineId)) {
          Stage.ConfigIssue issue = context.createConfigIssue(
              Groups.PIPELINE.name(),
              "conf.pipelineIdConfigList",
              StartPipelineErrors.START_PIPELINE_03,
              index
          );
          issues.add(issue);
          break;
        }
        index++;
      }
    }

    // Validate Server is reachable
    if (issues.size() == 0) {
      try {
        ApiClient apiClient = getApiClient();
        SystemApi systemApi = new SystemApi(apiClient);
        systemApi.getServerTime();
        managerApi = new ManagerApi(apiClient);
        storeApi = new StoreApi(apiClient);
      } catch (Exception ex) {
        LOG.error(ex.getMessage(), ex);
        issues.add(
            context.createConfigIssue(
                Groups.PIPELINE.getLabel(),
                "conf.baseUrl",
                StartPipelineErrors.START_PIPELINE_01,
                ex.getMessage(),
                ex
            )
        );
      }
    }

    return issues;
  }

  private ApiClient getApiClient() throws StageException {
    String authType = "form";
    if (conf.controlHubEnabled) {
      authType = "dpm";
    }
    ApiClient apiClient = new ApiClient(authType);
    apiClient.setUserAgent("Start Pipeline Processor");
    apiClient.setBasePath(conf.baseUrl + "/rest");
    apiClient.setUsername(conf.username.get());
    apiClient.setPassword(conf.password.get());
    apiClient.setDPMBaseURL(conf.controlHubUrl);
    apiClient.setSslContext(conf.tlsConfig.getSslContext());
    return apiClient;
  }

  public LinkedHashMap<String, Field> startPipelineInParallel(
      List<CompletableFuture<Field>> startPipelineFutures,
      ErrorRecordHandler errorRecordHandler
  ) throws ExecutionException, InterruptedException {
    // Create a combined Future using allOf()
    CompletableFuture<Void> allFutures = CompletableFuture.allOf(
        startPipelineFutures.toArray(new CompletableFuture[0])
    );

    CompletableFuture<LinkedHashMap<String, Field>> completableFuture = allFutures.thenApply(v -> {
      LinkedHashMap<String, Field> outputField = new LinkedHashMap<>();
      boolean success = true;
      for (CompletableFuture<Field> future: startPipelineFutures) {
        try {
          Field startPipelineOutputField = future.get();
          if (startPipelineOutputField != null) {
            LinkedHashMap<String, Field> fields = startPipelineOutputField.getValueAsListMap();
            Field pipelineIdField = fields.get("pipelineId");
            outputField.put(pipelineIdField.getValueAsString(), startPipelineOutputField);
            success &= fields.get("success").getValueAsBoolean();
          } else {
            success = false;
          }
        } catch (Exception ex) {
          LOG.error(ex.toString(), ex);
          errorRecordHandler.onError(StartPipelineErrors.START_PIPELINE_04, ex.toString(), ex);
        }
      }
      outputField.put("success", Field.create(success));
      return outputField;
    });

    return completableFuture.get();
  }
}
