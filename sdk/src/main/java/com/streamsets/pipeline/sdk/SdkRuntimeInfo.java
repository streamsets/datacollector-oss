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
import com.streamsets.datacollector.main.RuntimeInfo;

import java.util.List;

public class SdkRuntimeInfo extends RuntimeInfo {

  public SdkRuntimeInfo(String propertyPrefix, MetricRegistry metrics, List<? extends ClassLoader> stageLibraryClassLoaders) {
    super(RuntimeInfo.SDC_PRODUCT, propertyPrefix, metrics, stageLibraryClassLoaders);
    this.setBaseHttpUrl("no-where://");
  }

  @Override
  public void init() {
  }

  @Override
  public String getId() {
    return "";
  }

  @Override
  public String getMasterSDCId() {
    return "";
  }

  @Override
  public String getRuntimeDir() {
    return "";
  }

  @Override
  public boolean isClusterSlave() {
    return false;
  }
}
