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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;

import java.util.List;


public class SlaveRuntimeInfo extends RuntimeInfo {

  private String masterSDCId;
  private String id;
  private boolean isRemotePipeline;

  public SlaveRuntimeInfo(
      String propertyPrefix, MetricRegistry metrics, List<? extends ClassLoader> stageLibraryClassLoaders
  ) {
    super(RuntimeInfo.SDC_PRODUCT, propertyPrefix, metrics, stageLibraryClassLoaders);
  }

  @Override
  public void init() {
    //
  }

  @Override
  public String getRuntimeDir() {
    if (Boolean.getBoolean(propertyPrefix + ".testing-mode")) {
      return System.getProperty("user.dir") + "/target/runtime-" + getRandomUUID();
    } else {
      return System.getProperty("user.dir") + "/" + getRandomUUID();
    }
  }

  public void setMasterSDCId(String masterSDCId) {
    this.masterSDCId = masterSDCId;
  }

  @Override
  public String getMasterSDCId() {
    return this.masterSDCId;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  public boolean isRemotePipeline() {
    return isRemotePipeline;
  }

  public void setRemotePipeline(boolean remotePipeline) {
    isRemotePipeline = remotePipeline;
  }

  @Override
  public boolean isClusterSlave() {
    return true;
  }

  @Override
  public String getLibsExtraDir() {
    throw new UnsupportedOperationException();
  }
}
