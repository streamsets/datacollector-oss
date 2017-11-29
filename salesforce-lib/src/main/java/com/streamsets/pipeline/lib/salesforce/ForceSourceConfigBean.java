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

public class ForceSourceConfigBean extends ForceInputConfigBean {
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
      displayPosition = 72,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public boolean useBulkAPI;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Use PK Chunking",
      description = "Enables automatic primary key (PK) chunking for the bulk query job.",
      displayPosition = 74,
      dependsOn = "useBulkAPI",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public boolean usePKChunking;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "100000",
      min = 1,
      max = 250000,
      label = "Chunk Size",
      displayPosition = 76,
      dependsOn = "usePKChunking",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public int chunkSize;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Start Id",
      description = "Optional 15- or 18-character record ID to be used as the lower boundary for the first chunk. " +
          "If omitted, all records matching the query will be retrieved.",
      displayPosition = 78,
      dependsOn = "usePKChunking",
      triggeredByValue = "true",
      group = "QUERY"
  )
  public String startId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
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
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
          @Dependency(configName = "usePKChunking", triggeredByValues = "false")
      },
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
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
          @Dependency(configName = "usePKChunking", triggeredByValues = "false")
      },
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
      dependencies = {
          @Dependency(configName = "queryExistingData", triggeredByValues = "true"),
          @Dependency(configName = "usePKChunking", triggeredByValues = "false")
      },
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
      type = ConfigDef.Type.MODEL,
      label = "Subscription Type",
      description = "Select Push Topic (to subscribe to SObject record changes) or Platform Event.",
      defaultValue = "PUSH_TOPIC",
      displayPosition = 120,
      dependencies = {
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "true"),
      },
      group = "SUBSCRIBE"
  )
  @ValueChooserModel(SubscriptionTypeChooserValues.class)
  public SubscriptionType subscriptionType = SubscriptionType.PUSH_TOPIC;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Push Topic",
      description = "Push Topic name, for example AccountUpdates. The Push Topic must be defined in your Salesforce environment.",
      displayPosition = 125,
      dependencies = {
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "true"),
          @Dependency(configName = "subscriptionType", triggeredByValues = "PUSH_TOPIC"),
      },
      group = "SUBSCRIBE"
  )
  public String pushTopic;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Platform Event API Name",
      description = "Platform Event API Name, for example Low_Ink__e. The Platform Event must be defined in your Salesforce environment.",
      displayPosition = 125,
      dependencies = {
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "true"),
          @Dependency(configName = "subscriptionType", triggeredByValues = "PLATFORM_EVENT"),
      },
      group = "SUBSCRIBE"
  )
  public String platformEvent;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Replay Option",
      description = "Choose which events to receive when the pipeline first starts.",
      defaultValue = "NEW_EVENTS",
      displayPosition = 127,
      dependencies = {
          @Dependency(configName = "subscribeToStreaming", triggeredByValues = "true"),
          @Dependency(configName = "subscriptionType", triggeredByValues = "PLATFORM_EVENT"),
      },
      group = "SUBSCRIBE"
  )
  @ValueChooserModel(ReplayOptionChooserValues.class)
  public ReplayOption replayOption = ReplayOption.NEW_EVENTS;

  @ConfigDefBean(groups = {"FORCE", "QUERY", "SUBSCRIBE", "ADVANCED"})
  public BasicConfig basicConfig = new BasicConfig();
}
