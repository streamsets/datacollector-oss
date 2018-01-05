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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.pipeline.api.ProtoSource;
import com.streamsets.pipeline.api.Stage;

import java.util.Collections;
import java.util.List;

public class StageBean {
  private final StageDefinition definition;
  private final StageConfiguration conf;
  private final StageConfigBean systemConfigs;
  private final Stage stage;
  private final ClassLoaderReleaser classLoaderReleaser;
  private final List<ServiceBean> services;

  public StageBean(
    StageDefinition definition,
    StageConfiguration conf,
    StageConfigBean systemConfigs,
    Stage stage,
    ClassLoaderReleaser classLoaderReleaser,
    List<ServiceBean> services
  ) {
    this.definition = definition;
    this.conf = conf;
    this.systemConfigs = systemConfigs;
    this.stage = stage;
    this.classLoaderReleaser = classLoaderReleaser;
    this.services = Collections.unmodifiableList(services);
  }

  public StageDefinition getDefinition() {
    return definition;
  }

  public StageConfiguration getConfiguration() {
    return conf;
  }

  public StageConfigBean getSystemConfigs() {
    return systemConfigs;
  }

  public Stage getStage() {
    return stage;
  }

  public List<ServiceBean> getServices() {
    return services;
  }

  public ServiceBean getService(Class service) {
    for(ServiceBean serviceBean : services) {
      if(serviceBean.getDefinition().getProvides() == service) {
        return serviceBean;
      }
    }
    return null;
  }

  public void releaseClassLoader() {
    classLoaderReleaser.releaseStageClassLoader(stage.getClass().getClassLoader());
    services.forEach(ServiceBean::releaseClassLoader);
  }

  public boolean isSource() {
    return stage instanceof ProtoSource;
  }
}
