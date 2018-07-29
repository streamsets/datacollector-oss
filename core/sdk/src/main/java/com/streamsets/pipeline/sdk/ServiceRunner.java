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
package com.streamsets.pipeline.sdk;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.ServiceContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceRunner<S> extends ProtoRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceRunner.class);

  private final S service;
  private final Class<S> serviceClass;
  private final ServiceContext serviceContext;

  protected ServiceRunner(
    S service,
    Class<S> serviceClass,
    Map<String, String> stageSdcConf,
    Map<String, Object> configs,
    Map<String, Object> constants,
    String resourcesDir,
    RuntimeInfo runtimeInfo
  ) {
    this.service = service;
    this.serviceClass = serviceClass;

    try {
      configureObject(service, configs);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    this.serviceContext = new ServiceContext(
      new Configuration(),
      ElUtil.getConfigToElDefMap(serviceClass),
      constants,
      new EmailSender(new Configuration()),
      new MetricRegistry(),
      "myPipeline",
      "0",
      0,
      "stageName",
      "serviceName",
      resourcesDir
    );

    Configuration sdcConfiguration = new Configuration();
    stageSdcConf.forEach((k, v) -> sdcConfiguration.set("stage.conf_" + k, v));

    status = Status.CREATED;
  }

  public S getService() {
    return service;
  }

  public Class<S> getServiceClass() {
    return serviceClass;
  }

  public Service.Context getContext() {
    return serviceContext;
  }

  public List<ConfigIssue> runValidateConfigs() {
    try {
      LOG.debug("Service '{}' validateConfigs starts", serviceClass.getCanonicalName());
      ensureStatus(Status.CREATED);
      try {
        return ((Service)service).init(getContext());
      } finally {
        ((Service)service).destroy();
      }
    } finally {
      LOG.debug("Service '{}' validateConfigs done", serviceClass.getCanonicalName());
    }
  }

  @SuppressWarnings("unchecked")
  public void runInit() throws StageException {
    LOG.debug("Service '{}' init starts", serviceClass.getCanonicalName());
    ensureStatus(Status.CREATED);
    List<ConfigIssue> issues = ((Service)service).init(getContext());

    if (!issues.isEmpty()) {
      List<String> list = new ArrayList<>(issues.size());
      for (Stage.ConfigIssue issue : issues) {
        list.add(issue.toString());
      }
      throw new StageException(ContainerError.CONTAINER_0010, list);
    }

    status = Status.INITIALIZED;
    LOG.debug("Service '{}' init ends", serviceClass.getCanonicalName());
  }

  public void runDestroy() throws StageException {
    LOG.debug("Service '{}' destroy starts", serviceClass.getCanonicalName());
    ensureStatus(Status.INITIALIZED);
    ((Service)service).destroy();
    status = Status.DESTROYED;
    LOG.debug("Service '{}' destroy ends", serviceClass.getCanonicalName());
  }

  public static class Builder<S, B extends ServiceRunner.Builder> {
    final S service;
    final Class<S> serviceClass;
    final Map<String, String> stageSdcConf;
    final Map<String, Object> configs;
    final Map<String, Object> constants;
    String resourcesDir;
    RuntimeInfo runtimeInfo;

    public Builder(Class<S> serviceClass, S service) {
      this.service = service;
      this.serviceClass = serviceClass;
      configs = new HashMap<>();
      this.constants = new HashMap<>();
      this.stageSdcConf = new HashMap<>();
      this.runtimeInfo = new SdkRuntimeInfo("", null,  null);
    }

    public B setResourcesDir(String resourcesDir) {
      this.resourcesDir = resourcesDir;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B addConfiguration(String name, Object value) {
      configs.put(Utils.checkNotNull(name, "name"), value);
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B addStageSdcConfiguration(String name, String value) {
      stageSdcConf.put(Utils.checkNotNull(name, "name"), value);
      return (B) this;
    }

    public B addConstants(Map<String, Object> constants) {
      this.constants.putAll(constants);
      return (B) this;
    }

    public B setRuntimeInfo(RuntimeInfo runtimeInfo) {
      this.runtimeInfo = runtimeInfo;
      return (B) this;
    }

    public ServiceRunner build() {
      return new ServiceRunner<>(
        service,
        serviceClass,
        stageSdcConf,
        configs,
        constants,
        resourcesDir,
        runtimeInfo
      );
    }
  }
}
