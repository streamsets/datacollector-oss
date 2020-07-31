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
package com.streamsets.pipeline.stage.origin.mongodb.oplog;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.common.mongodb.Errors;

import java.util.List;

public class MongoDBOplogSourceConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Initial Timestamp (secs)",
      description = "Specify the initial timestamp in seconds. Leave -1 to opt out.",
      displayPosition = 1003,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MONGODB",
      min = -1
  )
  public int initialTs;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Initial Ordinal",
      description = "Specify the initial ordinal after timestamp. Leave -1 to Opt out.",
      displayPosition = 1004,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MONGODB",
      min = -1,
      max = Integer.MAX_VALUE
  )
  public int initialOrdinal;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Operation Types",
      defaultValue = "[\"INSERT\", \"UPDATE\", \"DELETE\"]",
      description="Oplog Operation types to read",
      displayPosition = 1005,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MONGODB"
  )
  @MultiValueChooserModel(OplogOpTypeChooserValues.class)
  public List<OplogOpType> filterOplogOpTypes;

  private static final String MONGO_DB_OPLOG_SOURCE_CONFIG_BEAN_PREFIX = "mongoDBOplogSourceConfigBean.";

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (initialTs == -1 && initialOrdinal != -1) {
      issues.add(
          context.createConfigIssue(
              "MONGODB",
              MONGO_DB_OPLOG_SOURCE_CONFIG_BEAN_PREFIX + "initialTs",
              Errors.MONGODB_32,
              "Initial Timestamp",
              "Initial Ordinal"
          )
      );
    }
    if (initialTs != -1 && initialOrdinal == -1) {
      issues.add(
          context.createConfigIssue(
              "MONGODB",
              MONGO_DB_OPLOG_SOURCE_CONFIG_BEAN_PREFIX +"initialOrdinal",
              Errors.MONGODB_32,
              "Initial Ordinal",
              "Initial Timestamp"
          )
      );
    }
  }
}
