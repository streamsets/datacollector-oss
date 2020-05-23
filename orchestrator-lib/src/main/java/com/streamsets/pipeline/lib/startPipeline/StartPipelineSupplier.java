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
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.model.MetricRegistryJson;
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.PipelineInfoJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.datacollector.client.model.SourceOffsetJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.Constants;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StartPipelineSupplier implements Supplier<Field> {

  private static final Logger LOG = LoggerFactory.getLogger(StartPipelineSupplier.class);
  private final StartPipelineConfig conf;
  private final PipelineIdConfig pipelineIdConfig;
  private final StoreApi storeApi;
  private final ManagerApi managerApi;
  private final ErrorRecordHandler errorRecordHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private PipelineConfigurationJson pipelineConfiguration = null;

  public StartPipelineSupplier(
      ManagerApi managerApi,
      StoreApi storeApi,
      StartPipelineConfig conf,
      PipelineIdConfig pipelineIdConfig,
      ErrorRecordHandler errorRecordHandler
  ) {
    this.managerApi = managerApi;
    this.storeApi = storeApi;
    this.conf = conf;
    this.pipelineIdConfig = pipelineIdConfig;
    this.errorRecordHandler = errorRecordHandler;
  }

  @Override
  public Field get() {
    try {

      if (pipelineIdConfig.pipelineIdType.equals(PipelineIdType.TITLE)) {
        // fetch Pipeline ID using GET pipelines REST API using query param filterText=<pipeline name>
        List<PipelineInfoJson> pipelines = storeApi.getPipelines(
            pipelineIdConfig.pipelineId,
            null,
            0,
            2,
            null,
            null,
            false
        );
        if (pipelines.size() != 1) {
          throw new StageException(
              StartPipelineErrors.START_PIPELINE_05,
              pipelineIdConfig.pipelineId,
              pipelines.size()
          );
        }
        pipelineIdConfig.pipelineId = pipelines.get(0).getPipelineId();
      }

      // validate pipelineId exists
      pipelineConfiguration = storeApi.getPipelineInfo(
          pipelineIdConfig.pipelineId,
          Constants.REV,
          null,
          false
      );

      if (conf.resetOrigin) {
        managerApi.resetOffset(pipelineIdConfig.pipelineId, Constants.REV);
      }
      Map<String, Object> runtimeParameters = null;
      if (StringUtils.isNotEmpty(pipelineIdConfig.runtimeParameters)) {
        runtimeParameters = objectMapper.readValue(pipelineIdConfig.runtimeParameters, Map.class);
      }
      PipelineStateJson pipelineStateJson = managerApi.startPipeline(
          pipelineIdConfig.pipelineId,
          Constants.REV,
          runtimeParameters
      );
      if (conf.runInBackground) {
        generateField(pipelineStateJson);
      } else {
        waitForPipelineCompletion();
      }
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
      errorRecordHandler.onError(StartPipelineErrors.START_PIPELINE_04, ex.toString(), ex);
    }
    return responseField;
  }

  private void waitForPipelineCompletion() throws Exception {
    PipelineStateJson pipelineStateJson = managerApi.getPipelineStatus(pipelineIdConfig.pipelineId, Constants.REV);
    if (Constants.PIPELINE_SUCCESS_STATES.contains(pipelineStateJson.getStatus())) {
      generateField(pipelineStateJson);
    } else if (Constants.PIPELINE_ERROR_STATES.contains(pipelineStateJson.getStatus())) {
      generateField(pipelineStateJson);
    } else {
      ThreadUtil.sleep(conf.waitTime);
      waitForPipelineCompletion();
    }
  }

  private void generateField(PipelineStateJson pipelineStateJson) throws Exception {
    // after done add status, offset and metrics to record
    LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
    startOutput.put(Constants.PIPELINE_ID_FIELD, Field.create(pipelineConfiguration.getPipelineId()));
    startOutput.put(Constants.PIPELINE_TITLE_FIELD, Field.create(pipelineConfiguration.getTitle()));
    startOutput.put(Constants.STARTED_SUCCESSFULLY_FIELD, Field.create(true));
    if (!conf.runInBackground) {
      startOutput.put(
          Constants.FINISHED_SUCCESSFULLY_FIELD,
          Field.create(Constants.PIPELINE_SUCCESS_STATES.contains(pipelineStateJson.getStatus()))
      );

      MetricRegistryJson jobMetrics = StartPipelineCommon.getPipelineMetrics(objectMapper, pipelineStateJson);
      startOutput.put(Constants.PIPELINE_METRICS_FIELD, CommonUtil.getMetricsField(jobMetrics));
    }
    startOutput.put(Constants.PIPELINE_STATUS_FIELD, Field.create(pipelineStateJson.getStatus().toString()));
    startOutput.put(Constants.PIPELINE_STATUS_MESSAGE_FIELD, Field.create(pipelineStateJson.getMessage()));
    if (!conf.runInBackground) {
      SourceOffsetJson sourceOffset = managerApi.getCommittedOffsets(pipelineIdConfig.pipelineId, Constants.REV);
      startOutput.put(Constants.COMMITTED_OFFSET_STR, Field.create(objectMapper.writeValueAsString(sourceOffset)));
    }
    responseField = Field.createListMap(startOutput);
  }
}
