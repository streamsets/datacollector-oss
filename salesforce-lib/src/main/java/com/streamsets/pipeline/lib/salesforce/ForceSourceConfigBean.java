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
import com.streamsets.pipeline.lib.el.OffsetEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;

public class ForceSourceConfigBean extends ForceConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Query Existing Data",
      description = "If enabled, existing data will be read from Force.com.",
      displayPosition = 70,
      group = "FORCE"
  )
  public boolean queryExistingData;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Bulk API",
      description = "If enabled, records will be read and written via the Salesforce Bulk API, " +
          "otherwise, the Salesforce SOAP API will be used.",
      displayPosition = 75,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public boolean useBulkAPI;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      defaultValue = "",
      label = "SOQL Query",
      description =
          "SELECT <offset field>, <more fields>, ... FROM <object name> WHERE <offset field>  >  ${OFFSET} ORDER BY <offset field>",
      elDefs = {OffsetEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 80,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public String soqlQuery;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include Deleted Records",
      description = "When enabled, the processor will additionally retrieve deleted records from the Recycle Bin",
      defaultValue = "false",
      displayPosition = 82,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public boolean queryAll = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NO_REPEAT",
      label = "Repeat Query",
      description = "Select one of the options to repeat the query, or not",
      displayPosition = 85,
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "false")
      },
      group = "QUERY"
  )
  @ValueChooserModel(ForceRepeatQueryChooserValues.class)
  public ForceRepeatQuery repeatQuery;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "${1 * MINUTES}",
      label = "Query Interval",
      displayPosition = 87,
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "false"),
          @Dependency(configName = "repeatQuery", triggeredByValues = {"FULL", "INCREMENTAL"}),
      },
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "QUERY"
  )
  public long queryInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "000000000000000",
      label = "Initial Offset",
      description = "Initial value to insert for ${offset}." +
          " Subsequent queries will use the result of the Next Offset Query",
      displayPosition = 90,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public String initialOffset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "Id",
      label = "Offset Field",
      description = "Field checked to track current offset.",
      displayPosition = 100,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public String offsetColumn;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Subscribe for Notifications",
      description = "If enabled, the origin will subscribe to the Force.com Streaming API for notifications.",
      displayPosition = 110,
      group = "FORCE"
  )
  public boolean subscribeToStreaming;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Push Topic",
      description = "Push Topic name, for example AccountUpdates. The Push Topic must be defined in your Salesforce environment.",
      displayPosition = 120,
      dependsOn = "subscribeToStreaming",
      triggeredByValue = "true",
      group = "SUBSCRIBE"
  )
  public String pushTopic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Create Salesforce Attributes",
      description = "Generates record header and field attributes that provide additional details about source data, such as the source object and original data type.",
      defaultValue = "true",
      displayPosition = 130,
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
      group = "ADVANCED",
      dependsOn = "createSalesforceNsHeaders",
      triggeredByValue = "true"
  )
  public String salesforceNsHeaderPrefix = "salesforce.";

  @ConfigDefBean(groups = {"FORCE", "QUERY", "SUBSCRIBE", "ADVANCED"})
  public BasicConfig basicConfig = new BasicConfig();
}
