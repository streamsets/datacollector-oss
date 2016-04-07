/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class MongoTargetConfigBean {

  // Available options for client URI - http://api.mongodb.org/java/3.0/?com/mongodb/MongoClientURI.html
  @ConfigDef(
    type = ConfigDef.Type.STRING,
    label = "Connection String",
    description = "Use format mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]",
    required = true,
    group = "MONGODB",
    displayPosition = 10
  )
  public String mongoClientURI;

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
    type = ConfigDef.Type.MODEL,
    label = "Unique Key Field",
    description = "Unique key field is required for upserts and optional for inserts and deletes",
    required = false,
    group = "MONGODB",
    displayPosition = 40
  )
  @FieldSelectorModel(singleValued = true)
  public String uniqueKeyField;

  @ConfigDef(
    type = ConfigDef.Type.MODEL,
    label = "Write Concern",
    defaultValue = "JOURNALED",
    required = true,
    group = "MONGODB",
    displayPosition = 50
  )
  @ValueChooserModel(WriteConcernChooserValues.class)
  public WriteConcernLabel writeConcern = WriteConcernLabel.JOURNALED;

}
