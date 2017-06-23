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

package com.streamsets.pipeline.stage.lib.sns;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AWSRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;

public class SNSConfig {
  @ConfigDefBean(groups = "SNS")
  public AWSConfig aws = new AWSConfig();

  @ConfigDefBean(groups = "ADVANCED")
  public ProxyConfig proxyConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Region",
      displayPosition = 10,
      group = "SNS"
  )
  @ValueChooserModel(AWSRegionChooserValues.class)
  public AWSRegions region;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = 15,
      dependsOn = "region",
      triggeredByValue = "OTHER",
      group = "SNS"
  )
  public String endpoint;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topic ARN",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = RecordEL.class,
      displayPosition = 20,
      group = "SNS"
  )
  public String topicArnExpression;
}
