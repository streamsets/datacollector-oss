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
package com.streamsets.pipeline.stage.destination.iothub;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.eventhubs.DestinationDataFormatChooserValues;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

public class IotHubProducerConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when writing records to Azure IoT Hub",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DestinationDataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "IoT Hub Name",
      defaultValue = "",
      description = "",
      displayPosition = 10,
      group = "IOT_HUB"
  )
  public String iotHubName = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Device ID",
      defaultValue = "",
      description = "",
      displayPosition = 20,
      group = "IOT_HUB"
  )
  public String deviceId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Shared Access Key",
      defaultValue = "",
      description = "Shared Access Primary Key string associated with the policy",
      displayPosition = 30,
      group = "IOT_HUB"
  )
  public String sasKey = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum Request Time (sec)",
      defaultValue = "300",
      description = "Maximum time to wait for each batch request completion.",
      displayPosition = 40,
      group = "IOT_HUB"
  )
  public long maxRequestCompletionSecs = 300;

}
