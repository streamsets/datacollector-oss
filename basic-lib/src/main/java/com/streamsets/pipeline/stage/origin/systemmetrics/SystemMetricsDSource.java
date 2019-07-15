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
package com.streamsets.pipeline.stage.origin.systemmetrics;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "System Metrics",
    description = "Reads information on system utilization (CPU, memory, disks, network)",
    execution = {ExecutionMode.EDGE},
    icon = "metrics.png",
    onlineHelpRefUrl ="", //TODO: Update help URL
    upgrader = SystemMetricsUpgrader.class,
    upgraderDef = "upgrader/SystemMetricsDSource.yaml"
)
@ConfigGroups(Groups.class)
public class SystemMetricsDSource extends DSource {

  @ConfigDefBean
  public SystemMetricsConfigBean conf;

  @Override
  protected Source createSource() {
    return new SystemMetricsSource();
  }
}
