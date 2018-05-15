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

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.interceptor.Interceptor;

public class InterceptorContext implements Interceptor.Context {

  private final BlobStore blobStore;
  private final Configuration configuration;
  private final String stageInstanceName;

  public InterceptorContext(
    BlobStore blobStore,
    Configuration configuration,
    String stageInstanceName
  ) {
    this.blobStore = blobStore;
    this.configuration = configuration;
    this.stageInstanceName = stageInstanceName;
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
}
