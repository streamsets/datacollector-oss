/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.OffsetEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;

public class ForceBulkConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use PK Chunking",
      description = "Enables automatic primary key (PK) chunking for the bulk query job. " +
          "Note that the 'Query All' option and offsets are not used with PK Chunking, " +
          "and the SOQL Query cannot contain an ORDER BY clause, or contain the Id field in a WHERE clause.",
      displayPosition = 74,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "useBulkAPI^",
      triggeredByValue = "true",
      group = "#0"
  )
  public boolean usePKChunking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "100000",
      min = 1,
      max = 250000,
      label = "Chunk Size",
      displayPosition = 76,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      dependsOn = "usePKChunking",
      triggeredByValue = "true",
      group = "#0"
  )
  public int chunkSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Start Id",
      description = "Optional 15- or 18-character record ID to be used as the lower boundary for the first chunk. " +
          "If omitted, all records matching the query will be retrieved.",
      displayPosition = 78,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "usePKChunking",
      triggeredByValue = "true",
      group = "#0"
  )
  public String startId;
}
