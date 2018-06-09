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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class InterceptorContext implements Interceptor.Context {

  private final StageLibraryTask stageLibrary;
  private final String pipelineId;
  private final String pipelineTitle;
  private final String rev;
  private final Stage.UserContext userContext;
  private final MetricRegistry metrics;
  private final long pipelineMaxMemory;
  private final ExecutionMode executionMode;
  private final DeliveryGuarantee deliveryGuarantee;
  private final RuntimeInfo runtimeInfo;
  private final EmailSender emailSender;
  private final long startTime;
  private final LineagePublisherDelegator lineagePublisherDelegator;
  private final BlobStore blobStore;
  private final Configuration configuration;
  private final String stageInstanceName;

  /**
   * Flag to configure if createStage method should be allowed or not.
   */
  private boolean allowCreateStage = false;
  public void setAllowCreateStage(boolean allowCreateStage) {
    this.allowCreateStage = allowCreateStage;
  }

  /**
   * List of issues for dependent stages.
   */
  private List issues = new ArrayList<>();

  public InterceptorContext(
    BlobStore blobStore,
    Configuration configuration,
    String stageInstanceName,
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
    long startTime,
    LineagePublisherDelegator lineagePublisherDelegator
  ) {
    this.blobStore = blobStore;
    this.configuration = configuration;
    this.stageInstanceName = stageInstanceName;
    this.stageLibrary = stageLibrary;
    this.pipelineId = pipelineId;
    this.pipelineTitle = pipelineTitle;
    this.rev = rev;
    this.userContext = userContext;
    this.metrics = metrics;
    this.pipelineMaxMemory = pipelineMaxMemory;
    this.executionMode = executionMode;
    this.deliveryGuarantee = deliveryGuarantee;
    this.runtimeInfo = runtimeInfo;
    this.emailSender = emailSender;
    this.startTime = startTime;
    this.lineagePublisherDelegator = lineagePublisherDelegator;
  }

  @Override
  public ConfigIssue createConfigIssue(ErrorCode errorCode, Object... args) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : ProtoContext.NULL_ONE_ARG;
    return new ProtoContext.ConfigIssueImpl(stageInstanceName, null, null, null, errorCode, args);
  }

  @Override
  public String getConfig(String configName) {
    return configuration.get(configName, null);
  }

  @Override
  public BlobStore getBlobStore() {
    return blobStore;
  }

  @Override
  public <S> S createStage(String jsonDefinition, Class<S> klass) {
    if(!allowCreateStage) {
      throw new IllegalStateException("Method createStage can only be called during initialization phase!");
    }

    if(!DetachedStageRuntime.supports(klass)) {
      throw new IllegalArgumentException("This runtime does not support " + klass.getName());
    }

    if(Strings.isNullOrEmpty(jsonDefinition)) {
      return null;
    }

    DetachedStageRuntime stageRuntime = DetachedStage.get().createDetachedStage(
      jsonDefinition,
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
      issues
    );

    if(stageRuntime == null) {
      return null;
    }

    // TODO: SDC-9239: Wire lifecycle of Detached stages in Interceptor context

    return (S)stageRuntime;
  }
}
