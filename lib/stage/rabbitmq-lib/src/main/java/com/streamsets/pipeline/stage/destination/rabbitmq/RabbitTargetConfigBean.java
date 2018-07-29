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
package com.streamsets.pipeline.stage.destination.rabbitmq;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.rabbitmq.config.BaseRabbitConfigBean;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

public class RabbitTargetConfigBean extends BaseRabbitConfigBean{
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "One Message per Batch",
      description = "Generates a single Rabbit MQ message with all records in the batch",
      displayPosition = 50,
      group = "#0"
  )
  public boolean singleMessagePerBatch = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Mandatory",
      description = "Publish Rabbit MQ message with mandatory flag set",
      displayPosition = 50,
      group = "#0"
  )
  public boolean mandatory = false;

  //Immediate flag is deprecated since Rabbit 3.

  @ConfigDefBean(groups = "RABBITMQ")
  public BasicPropertiesConfig basicPropertiesConfig = new BasicPropertiesConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataFormatConfig = new DataGeneratorFormatConfig();

}
