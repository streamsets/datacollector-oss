/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.CreationError;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.DetachedStageValidator;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Facility to create an instance of detached stage (e.g. stage that is not directly part of pipeline).
 */
public abstract class DetachedStage {
  private static final Logger LOG = LoggerFactory.getLogger(DetachedStage.class);

  private static final DetachedStage INSTANCE = new DetachedStage() {};

  public static DetachedStage get() {
    return INSTANCE;
  }

  /**
   * Create a new instance of a stage that does not directly live in the pipeline canvas.
   */
  public DetachedStageRuntime createDetachedStage(
    String jsonDefinition,
    StageLibraryTask stageLibrary,
    String pipelineId,
    String pipelineTitle,
    String rev,
    Stage.UserContext userContext,
    MetricRegistry metrics,
    long pipelineMaxMemory,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    RuntimeInfo runtimeInfo,
    EmailSender emailSender,
    Configuration configuration,
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator,
    List<Issue> errors
  ) {
    StageConfiguration stageConf;
    try {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      StageConfigurationJson stageConfJson = objectMapper.readValue(jsonDefinition, StageConfigurationJson.class);
      stageConf = stageConfJson.getStageConfiguration();
    } catch (IOException e) {
      LOG.error(CreationError.CREATION_0900.getMessage(), e.toString(), e);
      errors.add(IssueCreator.getPipeline().create(
        CreationError.CREATION_0900,
        e.toString()
      ));
      return null;
    }

    return createDetachedStage(
      stageConf,
      stageLibrary,
      pipelineId,
      pipelineTitle,
      rev,
      userContext,
      metrics,
      pipelineMaxMemory,
      executionMode,
      deliveryGuarantee,
      runtimeInfo,
      emailSender,
      configuration,
      startTime,
      lineagePublisherDelegator,
      errors
    );
  }

  /**
   * Create a new instance of a stage that does not directly live in the pipeline canvas.
   */
  public DetachedStageRuntime createDetachedStage(
    StageConfiguration stageConf,
    StageLibraryTask stageLibrary,
    String pipelineId,
    String pipelineTitle,
    String rev,
    Stage.UserContext userContext,
    MetricRegistry metrics,
    long pipelineMaxMemory,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    RuntimeInfo runtimeInfo,
    EmailSender emailSender,
    Configuration configuration,
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator,
    List<Issue> errors
  ) {
    // Firstly validate that the configuration is correct and up to date
    DetachedStageValidator validator = new DetachedStageValidator(stageLibrary, stageConf);
    stageConf = validator.validate();
    if(!errors.isEmpty()) {
      return null;
    }

    // Then stageBean that will create new instance and properly propagate all the
    StageBean stageBean = PipelineBeanCreator.get().createStageBean(
      true,
      stageLibrary,
      stageConf,
      false,
      false,
      Collections.emptyMap(),
      null,
      errors
    );
    if(!errors.isEmpty()) {
      return null;
    }

    // Stage.Info and Stage.Context
    Stage.Info stageInfo = new Stage.Info() {
      @Override
      public String getName() {
        return stageBean.getDefinition().getName();
      }

      @Override
      public int getVersion() {
        return stageBean.getDefinition().getVersion();
      }

      @Override
      public String getInstanceName() {
        return stageBean.getConfiguration().getInstanceName();
      }

      @Override
      public String getLabel() {
        return stageBean.getConfiguration().getInstanceName();
      }
    };

    StageContext context = new StageContext(
      pipelineId,
      pipelineTitle,
      rev,
      Collections.emptyList(),
      userContext,
      stageBean.getDefinition().getType(),
      0,
      false,
      metrics,
      stageBean.getDefinition().getConfigDefinitions(),
      stageBean.getSystemConfigs().stageOnRecordError,
      Collections.emptyList(),
      Collections.emptyMap(),
      stageInfo,
      pipelineMaxMemory,
      executionMode,
      deliveryGuarantee,
      runtimeInfo,
      emailSender,
      configuration,
      Collections.emptyMap(),
      startTime,
      lineagePublisherDelegator,
      Collections.emptyMap(),
      false
    );

    return new DetachedStageRuntime(stageBean, stageInfo, context);
  }

}
