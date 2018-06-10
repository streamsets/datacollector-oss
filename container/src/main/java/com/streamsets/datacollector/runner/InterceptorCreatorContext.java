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

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.api.interceptor.InterceptorCreator;

import java.util.Map;

public class InterceptorCreatorContext implements InterceptorCreator.Context {

  private final BlobStore blobStore;
  private final Configuration configuration;
  private final StageType stageType;
  private final InterceptorCreator.InterceptorType interceptorType;

  public InterceptorCreatorContext(
    BlobStore blobStore,
    Configuration configuration,
    StageType stageType,
    InterceptorCreator.InterceptorType interceptorType
  ) {
    this.blobStore = blobStore;
    this.configuration = configuration;
    this.stageType = stageType;
    this.interceptorType = interceptorType;
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
  public StageType getStageType() {
    return stageType;
  }

  @Override
  public InterceptorCreator.InterceptorType getInterceptorType() {
    return interceptorType;
  }

  @Override
  public Map<String, String> getParameters() {
    return null;
  }
}
