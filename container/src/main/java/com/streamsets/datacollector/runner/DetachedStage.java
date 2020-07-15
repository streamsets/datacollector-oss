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
import com.streamsets.datacollector.config.DetachedStageConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.CreationError;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.DetachedStageConfigurationJson;
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
import java.util.HashMap;
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
  public<S> DetachedStageRuntime<? extends S> createDetachedStage(
    String jsonDefinition,
    StageLibraryTask stageLibrary,
    String pipelineId,
    String pipelineTitle,
    String rev,
    Stage.UserContext userContext,
    MetricRegistry metrics,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    BuildInfo buildInfo,
    RuntimeInfo runtimeInfo,
    EmailSender emailSender,
    Configuration configuration,
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator,
    Class<S> klass,
    List<Issue> errors
  ) {
    DetachedStageConfiguration stageConf;
    try {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      DetachedStageConfigurationJson stageConfJson = objectMapper.readValue(jsonDefinition, DetachedStageConfigurationJson.class);
      stageConf = stageConfJson.getDetachedStageConfiguration();
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
      executionMode,
      deliveryGuarantee,
      buildInfo,
      runtimeInfo,
      emailSender,
      configuration,
      startTime,
      lineagePublisherDelegator,
      klass,
      errors
    );
  }

  /**
   * Create a new instance of a stage that does not directly live in the pipeline canvas.
   */
  public<S> DetachedStageRuntime<? extends S> createDetachedStage(
    DetachedStageConfiguration stageConf,
    StageLibraryTask stageLibrary,
    String pipelineId,
    String pipelineTitle,
    String rev,
    Stage.UserContext userContext,
    MetricRegistry metrics,
    ExecutionMode executionMode,
    DeliveryGuarantee deliveryGuarantee,
    BuildInfo buildInfo,
    RuntimeInfo runtimeInfo,
    EmailSender emailSender,
    Configuration configuration,
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator,
    Class<S> klass,
    List<Issue> errors
  ) {
    // Firstly validate that the configuration is correct and up to date
    DetachedStageValidator validator = new DetachedStageValidator(stageLibrary, stageConf);
    DetachedStageConfiguration detachedStageConfiguration =  validator.validate();

    // If the stage is not valid, we can't create instance of it
    if(detachedStageConfiguration.getIssues().hasIssues()) {
      errors.addAll(detachedStageConfiguration.getIssues().getIssues());
      return null;
    }

    // Then stageBean that will create new instance and properly propagate all the
    StageBean stageBean = PipelineBeanCreator.get().createStageBean(
      true,
      stageLibrary,
      stageConf.getStageConfiguration(),
      false,
      false,
      false,
      Collections.emptyMap(),
      null,
      userContext.getUser(),
      new HashMap<>(),
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
      null, //TODO. Will need to set here if this stage needs to publish lineage events
      rev,
      null, //TODO. Will need to set here if this stage needs to publish lineage events
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
      executionMode,
      deliveryGuarantee,
      buildInfo,
      runtimeInfo,
      emailSender,
      configuration,
      Collections.emptyMap(),
      startTime,
      lineagePublisherDelegator,
      Collections.emptyMap(),
      false,
      null,
      null,
      null,
      false
    );

    return DetachedStageRuntime.create(stageBean, stageInfo, context, klass);
  }

}
