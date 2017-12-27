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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.service.Service;

import java.util.Map;

public class ServiceContext extends ProtoContext implements Service.Context {

  // Production Run
  public ServiceContext(
      Configuration configuration,
      Map<String, Object> constants,
      EmailSender emailSender,
      MetricRegistry metrics,
      String pipelineId,
      String rev,
      String stageName,
      ServiceRuntime serviceRuntime,
      String serviceName,
      String resourceDir
  ) {
    super(
      configuration,
      getConfigToElDefMap(serviceRuntime.getServiceBean().getDefinition().getConfigDefinitions()),
      constants,
      emailSender,
      metrics,
      pipelineId,
      rev,
      stageName,
      null,
      serviceName,
      resourceDir
    );
  }

  // SDK
  public ServiceContext(
      Configuration configuration,
      Map<String, Class<?>[]> configToElDefMap,
      Map<String, Object> constants,
      EmailSender emailSender,
      MetricRegistry metrics,
      String pipelineId,
      String rev,
      String stageName,
      String serviceName,
      String resourceDir
  ) {
    super(
      configuration,
      configToElDefMap,
      constants,
      emailSender,
      metrics,
      pipelineId,
      rev,
      stageName,
      null,
      serviceName,
      resourceDir
    );
  }
}
