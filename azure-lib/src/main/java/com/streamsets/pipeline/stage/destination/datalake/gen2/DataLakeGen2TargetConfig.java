/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.datalake.gen2;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.stage.conf.DataLakeGen2BaseConfig;

/**
 * Contains configurations that apply only to the Generation 2 target connector
 */
public class DataLakeGen2TargetConfig extends DataLakeGen2BaseConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use Roll Attribute",
      description = "Closes the current file and creates a new file when processing a record with the specified roll attribute",
      displayPosition = 138,
      group = "OUTPUT_FILES"
  )
  public boolean rollIfHeader;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "roll",
      label = "Roll Attribute Name",
      description = "Name of the roll attribute",
      displayPosition = 138,
      group = "OUTPUT_FILES",
      dependsOn = "rollIfHeader",
      triggeredByValue = "true"
  )
  public String rollHeaderName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Validate Directory Permissions",
      description = "When checked, ADLS destination creates a test file in configured target directory to verify access privileges",
      displayPosition = 140,
      group = "OUTPUT_FILES"
  )
  public boolean checkPermission;

}
