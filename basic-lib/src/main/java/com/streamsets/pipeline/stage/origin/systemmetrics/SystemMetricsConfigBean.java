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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;

public class SystemMetricsConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2000",
      label = "Delay Between Batches",
      description = "Milliseconds to wait before sending the next batch",
      min = 0,
      max = Integer.MAX_VALUE,
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS"
  )
  public long delay;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch Host Information",
      description = "",
      displayPosition = 11,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "true"
  )
  public boolean fetchHostInfo = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch CPU Stats",
      description = "",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "true"
  )
  public boolean fetchCpuStats = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch Memory Stats",
      description = "",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "true"
  )
  public boolean fetchMemStats = true;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch Disk Stats",
      description = "",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "true"
  )
  public boolean fetchDiskStats = true;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch Network Stats",
      description = "",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "true"
  )
  public boolean fetchNetStats = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Fetch Process Stats",
      description = "",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "SYSTEM_METRICS",
      defaultValue = "false"
  )
  public boolean fetchProcessStats = false;

  @ConfigDefBean
  public ProcessConfigBean processConf = new ProcessConfigBean();
}
