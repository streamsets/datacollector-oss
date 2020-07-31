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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.rabbitmq.config.BaseRabbitConfigBean;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class RabbitSourceConfigBean extends BaseRabbitConfigBean {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Consumer Tag",
      defaultValue = "",
      description = "Leave blank to use an auto-generated tag.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String consumerTag = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "One Record per Message",
      description = "Generates a single Record for a Rabbit MQ message",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean produceSingleRecordPerMessage = false;

  @ConfigDefBean(groups = "RABBITMQ")
  public BasicConfig basicConfig = new BasicConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = "RABBITMQ")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

}
