/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.common.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.stage.lib.aws.AWSConfig;
import com.streamsets.pipeline.stage.lib.aws.AwsRegion;
import com.streamsets.pipeline.stage.lib.aws.AwsRegionChooserValues;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Amazon S3",
    type = AwsS3Connection.TYPE,
    description = "Connects to Amazon S3",
    version = 2,
    upgraderDef = "upgrader/AwsS3Connection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER}
)
@ConfigGroups(AwsS3ConnectionGroups.class)
public class AwsS3Connection {

  public static final String TYPE = "STREAMSETS_AWS_S3";

  @ConfigDefBean()
  public AWSConfig awsConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Specific Region",
      description = "Enables choosing a specific region or endpoint, instead of the S3 default global endpoint " +
          "(s3.amazonaws.com)",
      displayPosition = -98,
      group = "#0",
      defaultValue = "false"
  )
  public boolean useRegion = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "US_WEST_2",
      label = "Region",
      displayPosition = -95,
      dependsOn = "useRegion",
      triggeredByValue = "true",
      group = "#0"
  )
  @ValueChooserModel(AwsRegionChooserValues.class)
  public AwsRegion region;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Endpoint",
      description = "",
      defaultValue = "",
      displayPosition = -90,
      dependencies = {
          @Dependency(configName = "useRegion", triggeredByValues = "true"),
          @Dependency(configName = "region", triggeredByValues = "OTHER")
      },
      group = "#0"
  )
  public String endpoint;

  @ConfigDefBean(groups = "#1")
  public ProxyConfig proxyConfig;
}
