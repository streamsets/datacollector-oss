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
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.service.Service;

public class ServiceContext implements Service.Context {

  private final String stageName;
  private final String serviceName;

  public ServiceContext(
      String stageName,
      String serviceName
  ) {
    this.stageName = stageName;
    this.serviceName = serviceName;
  }

  private static class ConfigIssueImpl extends Issue implements Service.ConfigIssue {
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
  public Service.ConfigIssue createConfigIssue(
    String configGroup,
    String configName,
    ErrorCode errorCode,
    Object... args
  ) {
    Preconditions.checkNotNull(errorCode, "errorCode cannot be null");
    args = (args != null) ? args.clone() : NULL_ONE_ARG;
    return new ConfigIssueImpl(stageName, serviceName, configGroup, configName, errorCode, args);
  }

}
