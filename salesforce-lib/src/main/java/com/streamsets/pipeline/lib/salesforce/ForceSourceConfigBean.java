/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.lib.el.OffsetEL;
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
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      defaultValue = "SELECT Id, Name FROM Account WHERE Id > '${OFFSET}' ORDER BY Id",
      label = "SOQL Query",
      description =
          "SELECT <offset field>, ... FROM <object name> WHERE <offset field>  >  ${OFFSET} ORDER BY <offset field>",
      elDefs = {OffsetEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 80,
      dependsOn = "queryExistingData",
      triggeredByValue = "true",
      group = "FORCE"
  )
  public String soqlQuery;

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
      group = "FORCE"
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
      group = "FORCE"
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
      defaultValue = "AccountUpdates",
      label = "Push Topic",
      displayPosition = 120,
      dependsOn = "subscribeToStreaming",
      triggeredByValue = "true",
      group = "FORCE"
  )
  public String pushTopic;

  @ConfigDefBean(groups = "FORCE")
  public BasicConfig basicConfig = new BasicConfig();
}
