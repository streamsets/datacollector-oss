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
package com.streamsets.pipeline.stage.origin.lib;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;

import java.util.List;

public class BasicConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Max Batch Size (records)",
    description = "Max number of records per batch",
    displayPosition = 1000,
    group = "#0",
    displayMode = ConfigDef.DisplayMode.ADVANCED,
    min = 1,
    max = Integer.MAX_VALUE
  )
  public int maxBatchSize = 1000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a partial or empty batch",
      displayPosition = 1010,
      group = "#0",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxWaitTime = 2000;

  public void init(Stage.Context context, String group, String configPrefix, List<Stage.ConfigIssue> issues) {
    validate(context, group, configPrefix, issues);
  }

  private void validate(Stage.Context context, String group, String configPrefix, List<Stage.ConfigIssue> issues) {
    if (maxBatchSize < 1) {
      issues.add(context.createConfigIssue(group, configPrefix + "maxBatchSize", BasicErrors.BASIC_01, maxBatchSize));
    }
    if (maxWaitTime < 1) {
      issues.add(context.createConfigIssue(group, configPrefix + "maxWaitTime", BasicErrors.BASIC_02, maxWaitTime));
    }
  }
}
