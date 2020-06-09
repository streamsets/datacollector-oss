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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.client.ApiClient;
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.api.SystemApi;
import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class StartPipelineCommon {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineCommon.class);
  public static final String SSL_CONFIG_PREFIX = "conf.tlsConfig.";
  public ManagerApi managerApi;
  public StoreApi storeApi;
  private final StartPipelineConfig conf;
  private ELVars pipelineIdConfigVars;
  private ELEval pipelineIdEval;
  private ELEval runtimeParametersEval;
  private DefaultErrorRecordHandler errorRecordHandler;

  public StartPipelineCommon(StartPipelineConfig conf) {
    this.conf = conf;
  }

  public List<Stage.ConfigIssue> init(
      List<Stage.ConfigIssue> issues,
      DefaultErrorRecordHandler errorRecordHandler,
      Stage.Context context
  ) {
    this.errorRecordHandler = errorRecordHandler;
    pipelineIdConfigVars = context.createELVars();
    pipelineIdEval = context.createELEval("pipelineId");
    runtimeParametersEval = context.createELEval("runtimeParameters");

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
        ApiClient apiClient = getApiClient(
            conf.controlHubEnabled,
            conf.baseUrl,
            conf.username,
            conf.password,
            conf.controlHubUrl,
            conf.tlsConfig
        );
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

  public static ApiClient getApiClient(
      boolean controlHubEnabled,
      String baseUrl,
      CredentialValue username,
      CredentialValue password,
      String controlHubUrl,
      TlsConfigBean tlsConfig
  ) {
    String authType = "form";
    if (controlHubEnabled) {
      authType = "dpm";
    }
    ApiClient apiClient = new ApiClient(authType);
    apiClient.setUserAgent("Start Pipeline Processor");
    apiClient.setBasePath(baseUrl + "/rest");
    apiClient.setUsername(username.get());
    apiClient.setPassword(password.get());
    apiClient.setDPMBaseURL(controlHubUrl);
    apiClient.setSslContext(tlsConfig.getSslContext());
    return apiClient;
  }

  public StartPipelineSupplier getStartPipelineSupplier(
      PipelineIdConfig pipelineIdConfig,
      Record record
  ) {
    PipelineIdConfig resolvedPipelineIdConfig = new PipelineIdConfig();
    resolvedPipelineIdConfig.pipelineIdType = pipelineIdConfig.pipelineIdType;
    if (record != null) {
      RecordEL.setRecordInContext(pipelineIdConfigVars, record);
    }
    TimeNowEL.setTimeNowInContext(pipelineIdConfigVars, new Date());
    resolvedPipelineIdConfig.pipelineId = pipelineIdEval.eval(
        pipelineIdConfigVars,
        pipelineIdConfig.pipelineId,
        String.class
    );
    resolvedPipelineIdConfig.runtimeParameters = runtimeParametersEval.eval(
        pipelineIdConfigVars,
        pipelineIdConfig.runtimeParameters,
        String.class
    );
    return new StartPipelineSupplier(
        managerApi,
        storeApi,
        conf,
        resolvedPipelineIdConfig,
        errorRecordHandler
    );
  }

  public LinkedHashMap<String, Field> startPipelineInParallel(
      List<CompletableFuture<Field>> startPipelineFutures
  ) throws ExecutionException, InterruptedException {
    // Create a combined Future using allOf()
    CompletableFuture<Void> allFutures = CompletableFuture.allOf(
        startPipelineFutures.toArray(new CompletableFuture[0])
    );

    CompletableFuture<LinkedHashMap<String, Field>> completableFuture = allFutures.thenApply(v -> {
      LinkedHashMap<String, Field> pipelineResults = new LinkedHashMap<>();
      List<Field> pipelineIds = new ArrayList<>();
      boolean success = true;
      for (CompletableFuture<Field> future: startPipelineFutures) {
        try {
          Field startPipelineOutputField = future.get();
          if (startPipelineOutputField != null) {
            LinkedHashMap<String, Field> fields = startPipelineOutputField.getValueAsListMap();
            Field pipelineIdField = fields.get("pipelineId");
            if (pipelineIdField != null) {
              pipelineIds.add(Field.create(pipelineIdField.getValueAsString()));
              pipelineResults.put(pipelineIdField.getValueAsString(), startPipelineOutputField);
            }
            if (!conf.runInBackground) {
              Field finishedSuccessfully = fields.get(Constants.FINISHED_SUCCESSFULLY_FIELD);
              if (finishedSuccessfully != null) {
                success &= finishedSuccessfully.getValueAsBoolean();
              }
            }
          } else {
            success = false;
          }
        } catch (Exception ex) {
          LOG.error(ex.toString(), ex);
          errorRecordHandler.onError(StartPipelineErrors.START_PIPELINE_04, ex.toString(), ex);
        }
      }
      LinkedHashMap<String, Field> outputField = new LinkedHashMap<>();
      outputField.put(Constants.PIPELINE_IDS_FIELD, Field.create(pipelineIds));
      outputField.put(Constants.PIPELINE_RESULTS_FIELD, Field.createListMap(pipelineResults));
      if (!conf.runInBackground) {
        // Don't set success flag for the task if the runInBackground set
        outputField.put(Constants.SUCCESS_FIELD, Field.create(success));
      }
      return outputField;
    });

    return completableFuture.get();
  }


  public static MetricRegistryJson getPipelineMetrics(
      ObjectMapper objectMapper,
      PipelineStateJson pipelineStateJson
  ) {
    if (StringUtils.isNotEmpty(pipelineStateJson.getMetrics())) {
      try {
        return objectMapper.readValue(pipelineStateJson.getMetrics(), MetricRegistryJson.class);
      } catch (IOException ex) {
        LOG.warn("Error while serializing pipeline metrics JSON string: , {}", ex.toString(), ex);
      }
    }
    return null;
  }
}
