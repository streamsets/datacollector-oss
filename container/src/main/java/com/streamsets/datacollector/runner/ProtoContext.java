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

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;

/**
 * Shared context for both Service and Stage.
 */
public class ProtoContext implements ProtoConfigurableEntity.Context {

  private final String stageInstanceName;
  private final String serviceInstanceName;
  private final String resourcesDir;

  protected ProtoContext(
      String stageInstanceName,
      String serviceInstanceName,
      String resourcesDir
  ) {
    this.stageInstanceName = stageInstanceName;
    this.serviceInstanceName = serviceInstanceName;
    this.resourcesDir = resourcesDir;
  }

  private static class ConfigIssueImpl extends Issue implements ConfigIssue {
    public ConfigIssueImpl(
        String stageName,
        String serviceName,
        String configGroup,
        String configName,
        ErrorCode errorCode,
        Object... args
    ) {
      super(stageName, serviceName, configGroup, configName, errorCode, args);
    }
  }

  private static final Object[] NULL_ONE_ARG = {null};


  @Override
  public String getResourcesDirectory() {
    return resourcesDir;
  }

  @Override
  public Record createRecord(String recordSourceId) {
    return new RecordImpl(stageInstanceName, recordSourceId, null, null);
  }

  @Override
  public Record createRecord(String recordSourceId, byte[] raw, String rawMime) {
    return new RecordImpl(stageInstanceName, recordSourceId, raw, rawMime);
  }

  @Override
  public ConfigIssue createConfigIssue(
    String configGroup,
    String configName,
    ErrorCode errorCode,
    Object... args
  ) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(stageInstanceName, serviceInstanceName, configGroup, configName, errorCode, args);
  }
}
