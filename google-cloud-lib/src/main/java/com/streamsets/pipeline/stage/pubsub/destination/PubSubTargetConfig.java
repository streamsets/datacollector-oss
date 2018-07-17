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
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.GoogleCloudCredentialsConfig;
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
      group = "PUBSUB"
  )
  public String topicId;

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
  public GoogleCloudCredentialsConfig credentials = new GoogleCloudCredentialsConfig();
}
