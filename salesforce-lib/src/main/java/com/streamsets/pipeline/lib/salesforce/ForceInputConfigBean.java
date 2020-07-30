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
package com.streamsets.pipeline.lib.salesforce;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;

/**
 Common to origin and lookup processor
 */
public class ForceInputConfigBean extends ForceConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Query Existing Data",
      description = "If enabled, existing data will be read from Force.com.",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "FORCE"
  )
  public boolean queryExistingData;

  @ConfigDefBean(groups = {"QUERY"})
  public ForceBulkConfigBean bulkConfig = new ForceBulkConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include Deleted Records",
      description = "When enabled, the processor will additionally retrieve deleted records from the Recycle Bin",
      defaultValue = "false",
      displayPosition = 82,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
      },
      group = "QUERY"
  )
  public boolean queryAll = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Create Salesforce Attributes",
      description = "Generates record header and field attributes that provide additional details about source data, such as the source object and original data type.",
      defaultValue = "true",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  public boolean createSalesforceNsHeaders = true;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Salesforce Attribute Prefix",
      description = "Prefix for the header and field attributes, used as follows: <prefix>.<type of information>. For example: salesforce.precision and salesforce.scale",
      defaultValue = "salesforce.",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED",
      dependsOn = "createSalesforceNsHeaders",
      triggeredByValue = "true"
  )
  public String salesforceNsHeaderPrefix = "salesforce.";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mismatched Types Behavior",
      description = "How to handle fields with types that do not match the schema.",
      defaultValue = "PRESERVE_DATA",
      displayPosition = 310,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "ADVANCED"
  )
  @ValueChooserModel(MismatchedTypesOptionChooserValues.class)
  public MismatchedTypesOption mismatchedTypesOption = MismatchedTypesOption.PRESERVE_DATA;
}
