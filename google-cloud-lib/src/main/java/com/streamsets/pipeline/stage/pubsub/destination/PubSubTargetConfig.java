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

package com.streamsets.pipeline.stage.pubsub.destination;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.googlecloud.PubSubCredentialsConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.pubsub.origin.DataFormatChooserValues;

public class PubSubTargetConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataFormatConfig = new DataGeneratorFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topic ID",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PUBSUB"
  )
  public String topicId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Bytes Threshold",
      description = "After this many bytes are accumulated, the messages will be sent as a batch",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "1000",
      min = 1,
      group = "ADVANCED")
  public long requestBytesThreshold;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Messages Count Threshold",
      description = "After this many messages are accumulated, they will be sent as a batch",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "100",
      min = 1,
      group = "ADVANCED")
  public long elementsCountThreshold;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Default Delay Threshold (ms)",
      description = "After this amount of time has elapsed (counting from the first message added), the messages will" +
          " be sent as a batch. This value should not be set too high, usually on the order of milliseconds. " +
          "Otherwise, calls might appear to never complete",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "1",
      min = 1,
      group = "ADVANCED")
  public long defaultDelayThreshold;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Batch Enabled",
      description = "Set to send messages as a batch. If set to false, the batch logic will be disabled and the " +
          "simple API call will be used",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public boolean batchingEnabled;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Outstanding Message Count",
      description = "Maximum number of unprocessed messages to keep in memory before enforcing flow control",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "1000",
      min = 1,
      group = "ADVANCED")
  public long maxOutstandingElementCount;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Outstanding Request Bytes",
      description = "Maximum number of unprocessed bytes to keep in memory before enforcing flow control",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "8000",
      min = 1,
      group = "ADVANCED")
  public long maxOutstandingRequestBytes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "BLOCK",
      label = "Limit Exceeded Behaviour",
      description = "Specify the behavior of the flow controller when the specified limits are exceeded",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  @ValueChooserModel(LimitExceededBehaviourChooserValues.class)
  public LimitExceededBehaviour limitExceededBehavior;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Write Delimiter",
      description = "Should be checked when a delimiter must be written between messages. When unchecked " +
          "only a single message must be written to the destination file/Kafka message, etc.",
      displayPosition = 445,
      group = "DATA_FORMAT",
      dependsOn = "dataFormat",
      triggeredByValue = "PROTOBUF"
  )
  public boolean isDelimited;

  @ConfigDefBean(groups = "CREDENTIALS")
  public PubSubCredentialsConfig credentials = new PubSubCredentialsConfig();
}
