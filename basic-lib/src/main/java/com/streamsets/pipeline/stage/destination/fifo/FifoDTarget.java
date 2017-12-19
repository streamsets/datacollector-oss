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
package com.streamsets.pipeline.stage.destination.fifo;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

@ConfigGroups(Groups.class)
@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Named Pipe",
    description = "Sends records to a Named Pipe",
    icon="fifo.png",
    onlineHelpRefUrl = "index.html#Destinations/NamedPipe.html#task_pdv_vdg_gcb"
)

public class FifoDTarget extends DTarget {

  @ConfigDef(required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Named Pipe",
      description = "Full path of the Named Pipe",
      displayPosition = 10,
      group = "NAMED_PIPE"
  )
  public String namedPipe;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the files",
      displayPosition = 10,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @Override
  protected Target createTarget() {
    return new FifoTarget(namedPipe, dataFormat, dataGeneratorFormatConfig);
  }
}
