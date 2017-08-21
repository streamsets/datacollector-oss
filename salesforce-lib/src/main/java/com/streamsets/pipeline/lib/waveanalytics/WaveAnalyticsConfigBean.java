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
package com.streamsets.pipeline.lib.waveanalytics;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.lib.salesforce.ForceConfigBean;

public class WaveAnalyticsConfigBean extends ForceConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Edgemart Alias",
      description = "The alias of a dataset, which must be unique across an organization.",
      displayPosition = 50,
      group = "FORCE"
  )
  public String edgemartAliasPrefix;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Dataset Wait Time (secs)",
      description = "Max time to wait for new data before requesting that the dataset be processed.",
      displayPosition = 60,
      group = "FORCE"
  )
  public int datasetWaitTime = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Dataflow",
      description = "Enable to use a dataflow to combine successive datasets into one.",
      displayPosition = 70,
      group = "FORCE"
  )
  public boolean useDataflow;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "SalesEdgeEltWorkflow",
      label = "Dataflow Name",
      description = "Name of a dataflow to combine datasets into one. CAUTION - existing dataflow content will be overwritten!",
      displayPosition = 80,
      dependsOn = "useDataflow",
      triggeredByValue = "true",
      group = "FORCE"
  )
  public String dataflowName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Run Dataflow After Upload",
      description = "Enable this to run the dataflow after each dataset is uploaded. Caution - ensure that your Dataset Wait Time is at least an hour or you will overrun the limit on dataflow runs!",
      displayPosition = 90,
      dependsOn = "useDataflow",
      triggeredByValue = "true",
      group = "FORCE"
  )
  public boolean runDataflow = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      defaultValue = "",
      label = "Metadata JSON",
      description = "Metadata in JSON format, which describes the structure of the uploaded file.",
      displayPosition = 100,
      group = "FORCE"
  )
  public String metadataJson = "";
}
