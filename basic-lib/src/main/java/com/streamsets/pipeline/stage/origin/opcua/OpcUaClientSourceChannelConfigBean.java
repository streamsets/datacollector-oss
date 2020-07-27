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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.ConfigDef;

public class OpcUaClientSourceChannelConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Chunk Size",
      description = "The maximum size of a single chunk. Must be greater than or equal to 8192",
      defaultValue = "65536",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 8192,
      group = "CHANNEL_CONFIG"
  )
  public int maxChunkSize = 65536;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Chunk Count",
      description = "The maximum number of chunks that a message can break down into.",
      defaultValue = "32",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "CHANNEL_CONFIG"
  )
  public int maxChunkCount = 32;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Message Size",
      description = "The maximum size of a message after all chunks have been assembled. " +
          "Default value Max Chunk Size *  Max Chunk Count = (2mb)",
      defaultValue = "2097152",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 8192,
      group = "CHANNEL_CONFIG"
  )
  public int maxMessageSize = 2097152;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Array Length",
      defaultValue = "65536",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "CHANNEL_CONFIG"
  )
  public int maxArrayLength = 65536;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max String Length",
      defaultValue = "65536",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      min = 0,
      group = "CHANNEL_CONFIG"
  )
  public int maxStringLength = 65536;

}
