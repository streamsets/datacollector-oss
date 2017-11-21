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
package com.streamsets.pipeline.stage.origin.sensorreader;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class SensorReaderConfigBean {
  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BMxx80",
      label = "Sensor Device",
      description = "Sensor Device Family",
      displayPosition = 10,
      group = "SENSOR"
  )
  @ValueChooserModel(SensorDeviceChooserValues.class)
  public SensorDevice sensorDevice = SensorDevice.BMxx80;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "0x77",
      label = "I2C Address",
      description = "I2C Address in hexadecimal string",
      displayPosition = 20,
      group = "SENSOR"
  )
  public String i2cAddress = "0x77";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Delay Between Batches",
      description = "Milliseconds to wait before sending the next batch",
      min = 0,
      max = Integer.MAX_VALUE,
      displayPosition = 30,
      group = "SENSOR"
  )
  public long delay;
}
