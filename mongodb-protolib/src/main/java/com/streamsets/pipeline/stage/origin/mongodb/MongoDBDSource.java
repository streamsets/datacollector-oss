/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DSource;
import com.streamsets.pipeline.lib.el.TimeEL;

@StageDef(
    version = 1,
    label = "MongoDB",
    description = "Reads records from a MongoDB collection",
    icon="mongodb.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MongoDBDSource extends DSource {

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Connection String",
      required = true,
      group = "MONGODB",
      displayPosition = 10
  )
  public String mongoConnectionString;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Database",
      required = true,
      group = "MONGODB",
      displayPosition = 20
  )
  public String database;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Collection",
      required = true,
      group = "MONGODB",
      displayPosition = 30
  )
  public String collection;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Capped Collection",
      description = "Un-check this box if querying an uncapped collection.",
      displayPosition = 35,
      group = "MONGODB"
  )
  public boolean isCapped;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "2015-01-01 00:00:00",
      label = "Start Timestamp",
      description = "Provide in format: YYYY-MM-DD HH:mm:ss. Oldest data to be retrieved.",
      displayPosition = 40,
      group = "MONGODB"
  )
  public String initialOffset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "_id",
      label = "Offset Field",
      description = "Field checked to track current offset. Must be an ObjectId.",
      displayPosition = 50,
      group = "MONGODB"
  )
  public String offsetField;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Batch Size (records)",
      defaultValue = "1000",
      required = true,
      min = 2, // Batch size of 1 in MongoDB is special and analogous to LIMIT 1
      group = "MONGODB",
      displayPosition = 60
  )
  public int batchSize;

  @ConfigDef(
      type = ConfigDef.Type.NUMBER,
      label = "Max Batch Wait Time",
      defaultValue = "${5 * SECONDS}",
      required = true,
      elDefs = {TimeEL.class},
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      group = "MONGODB",
      displayPosition = 70
  )
  public long maxBatchWaitTime;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      required = true,
      group = "CREDENTIALS",
      displayPosition = 80
  )
  @ValueChooser(AuthenticationTypeChooserValues.class)
  public AuthenticationType authenticationType;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Username",
      required = true,
      dependsOn = "authenticationType",
      triggeredByValue = "USER_PASS",
      group = "CREDENTIALS",
      displayPosition = 90
  )
  public String username;

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      label = "Password",
      required = true,
      dependsOn = "authenticationType",
      triggeredByValue = "USER_PASS",
      group = "CREDENTIALS",
      displayPosition = 90
  )
  public String password;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Read Preference",
      defaultValue = "SECONDARY_PREFERRED",
      required = true,
      group = "ADVANCED",
      displayPosition = 150
  )
  @ValueChooser(ReadPreferenceChooserValues.class)
  public ReadPreferenceLabel readPreference;

  @Override
  protected Source createSource() {
    return new MongoDBSource(
        mongoConnectionString,
        database,
        collection,
        isCapped,
        offsetField,
        initialOffset,
        batchSize,
        maxBatchWaitTime,
        authenticationType,
        username,
        password,
        readPreference.getReadPreference()
    );
  }
}
