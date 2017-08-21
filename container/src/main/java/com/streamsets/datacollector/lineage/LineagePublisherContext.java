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
package com.streamsets.datacollector.lineage;

import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.lineage.LineagePublisher;

public class LineagePublisherContext implements LineagePublisher.Context {

  private final String id;
  private final Configuration configuration;
  private final String confPrefix;

  public LineagePublisherContext(
    String id,
    Configuration configuration
  ) {
    this.id = id;
    this.configuration = configuration;
    this.confPrefix = LineagePublisherConstants.configConfig(id);
  }

  @Override
  public String getId() {
    return id;
  }

  public static class ConfigIssueImpl implements LineagePublisher.ConfigIssue {
    private final ErrorMessage errorMessage;

    public ConfigIssueImpl(ErrorCode errorCode, Object... args) {
      this.errorMessage = new ErrorMessage(errorCode, args);
    }
  }

  @Override
  public LineagePublisher.ConfigIssue createConfigIssue(ErrorCode errorCode, Object... args) {
    return new ConfigIssueImpl(errorCode, args);
  }

  @Override
  public String getConfig(String configName) {
    return configuration.get(confPrefix + configName, null);
  }
}
