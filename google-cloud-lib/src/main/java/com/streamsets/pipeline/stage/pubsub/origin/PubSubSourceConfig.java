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

package com.streamsets.pipeline.stage.pubsub.origin;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.googlecloud.PubSubCredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class PubSubSourceConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the topic",
      displayPosition = 1,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Subscription ID",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PUBSUB"
  )
  public String subscriptionId;

  @ConfigDefBean(groups = "PUBSUB")
  public BasicConfig basic = new BasicConfig();

  @ConfigDefBean(groups = "ADVANCED")
  public PubSubAdvancedConfig advanced = new PubSubAdvancedConfig();

  @ConfigDefBean
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Num Pipeline Runners",
      description = "Maximum number of pipeline runners. Sets the parallelism of the pipeline.",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxThreads;

  @ConfigDefBean(groups = "CREDENTIALS")
  public PubSubCredentialsConfig credentials = new PubSubCredentialsConfig();
}
