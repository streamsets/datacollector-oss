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
package com.streamsets.pipeline.stage.origin.eventhubs;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.eventhubs.DataFormatChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class EventHubConsumerConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Event Hub payload data format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = "DATA_FORMAT")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "$Default",
      label = "Consumer Group",
      description = "The consumer group name that this receiver should be grouped under",
      displayPosition = 100,
      group = "EVENT_HUB"
  )
  public String consumerGroup = "$default";


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${pipeline:id()}",
      label = "Event Processor Prefix",
      description = "Enter a prefix unique to the pipeline. Used when communicating with Azure Event Hub.",
      displayPosition = 110,
      group = "EVENT_HUB"
  )
  public String hostNamePrefix;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Storage Account Name",
      description = "",
      displayPosition = 120,
      group = "EVENT_HUB"
  )
  public String storageAccountName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Storage Account Key",
      description = "",
      displayPosition = 130,
      group = "EVENT_HUB"
  )
  public CredentialValue storageAccountKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Container Name",
      description = "Used to store offset information. You must use a separate container for each pipeline.",
      displayPosition = 140,
      group = "EVENT_HUB"
  )
  public String storageContainerName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Max Threads",
      description = "Maximum number of record processing threads to spawn",
      displayPosition = 150,
      group = "EVENT_HUB",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxThreads;
}
