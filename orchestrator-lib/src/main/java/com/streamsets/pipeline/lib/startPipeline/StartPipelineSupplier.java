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
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.client.api.ManagerApi;
import com.streamsets.datacollector.client.api.StoreApi;
import com.streamsets.datacollector.client.model.PipelineConfigurationJson;
import com.streamsets.datacollector.client.model.PipelineStateJson;
import com.streamsets.datacollector.client.model.SourceOffsetJson;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StartPipelineSupplier implements Supplier<Field> {

  private static final String REV = "0";
  private final StartPipelineConfig conf;
  private final PipelineIdConfig pipelineIdConfig;
  private final StoreApi storeApi;
  private final ManagerApi managerApi;
  private final Stage.Context context;
  private ObjectMapper objectMapper = new ObjectMapper();
  private Field responseField = null;
  private PipelineConfigurationJson pipelineConfiguration = null;

  private List<PipelineStateJson.StatusEnum> successStates = ImmutableList.of(
      PipelineStateJson.StatusEnum.STOPPED,
      PipelineStateJson.StatusEnum.FINISHED
  );

  private List<PipelineStateJson.StatusEnum> errorStates = ImmutableList.of(
      PipelineStateJson.StatusEnum.START_ERROR,
      PipelineStateJson.StatusEnum.RUN_ERROR,
      PipelineStateJson.StatusEnum.STOP_ERROR,
      PipelineStateJson.StatusEnum.DISCONNECTED
  );

  public StartPipelineSupplier(
      ManagerApi managerApi,
      StoreApi storeApi,
      StartPipelineConfig conf,
      PipelineIdConfig pipelineIdConfig,
      Stage.Context context
  ) {
    this.managerApi = managerApi;
    this.storeApi = storeApi;
    this.conf = conf;
    this.pipelineIdConfig = pipelineIdConfig;
    this.context = context;
  }

  @Override
  public Field get() {
    try {
      // validate pipelineId exists
      pipelineConfiguration = storeApi.getPipelineInfo(pipelineIdConfig.pipelineId, REV, null, false);

      if (conf.resetOrigin) {
        managerApi.resetOffset(pipelineIdConfig.pipelineId, REV);
      }
      Map<String, Object> runtimeParameters = null;
      if (StringUtils.isNotEmpty(pipelineIdConfig.runtimeParameters)) {
        runtimeParameters = objectMapper.readValue(pipelineIdConfig.runtimeParameters, Map.class);
      }
      PipelineStateJson pipelineStateJson = managerApi.startPipeline(
          pipelineIdConfig.pipelineId,
          REV,
          runtimeParameters
      );
      if (conf.runInBackground) {
        generateField(pipelineStateJson);
      } else {
        waitForPipelineCompletion();
      }
    } catch (Exception ex) {
      context.reportError(ex);
    }
    return responseField;
  }

  private void waitForPipelineCompletion() throws Exception {
    PipelineStateJson pipelineStateJson = managerApi.getPipelineStatus(pipelineIdConfig.pipelineId, REV);
    if (successStates.contains(pipelineStateJson.getStatus())) {
      generateField(pipelineStateJson);
    } else if (errorStates.contains(pipelineStateJson.getStatus())) {
      generateField(pipelineStateJson);
    } else {
      ThreadUtil.sleep(conf.waitTime);
      waitForPipelineCompletion();
    }
  }

  private void generateField(PipelineStateJson pipelineStateJson) throws Exception {
    // after done add status, offset and metrics to record
    SourceOffsetJson sourceOffset = managerApi.getCommittedOffsets(pipelineIdConfig.pipelineId, REV);
    LinkedHashMap<String, Field> startOutput = new LinkedHashMap<>();
    startOutput.put("pipelineId", Field.create(pipelineConfiguration.getPipelineId()));
    startOutput.put("pipelineTitle", Field.create(pipelineConfiguration.getTitle()));
    startOutput.put("success", Field.create(successStates.contains(pipelineStateJson.getStatus())));
    startOutput.put("pipelineStatus", Field.create(pipelineStateJson.getStatus().toString()));
    startOutput.put("pipelineStatusMessage", Field.create(pipelineStateJson.getMessage()));
    startOutput.put("committedOffsetsStr", Field.create(objectMapper.writeValueAsString(sourceOffset)));
    startOutput.put("pipelineStateStr", Field.create(objectMapper.writeValueAsString(pipelineStateJson)));
    responseField = Field.createListMap(startOutput);
  }
}
