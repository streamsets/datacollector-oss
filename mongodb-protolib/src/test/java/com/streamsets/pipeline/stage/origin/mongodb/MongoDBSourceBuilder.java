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

package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

public class MongoDBSourceBuilder {
  private MongoSourceConfigBean config;

  public MongoDBSourceBuilder() {
    config = new MongoSourceConfigBean();
    config.mongoConfig = new MongoDBConfig();
    config.mongoConfig.connectionString = "mongodb://";
    config.mongoConfig.database = "database";
    config.mongoConfig.collection = "collection";
    config.mongoConfig.authenticationType = AuthenticationType.NONE;
    config.mongoConfig.username = null;
    config.mongoConfig.password = null;
    config.isCapped = true;
    config.offsetField = "_id";
    config.initialOffset = "0";
    config.batchSize = 100;
    config.maxBatchWaitTime = 1;
    config.readPreference = ReadPreferenceLabel.NEAREST;
  }

  public MongoDBSourceBuilder connectionString(String connectionString) {
    config.mongoConfig.connectionString = connectionString;
    return this;
  }

  public MongoDBSourceBuilder database(String database) {
    config.mongoConfig.database = database;
    return this;
  }

  public MongoDBSourceBuilder collection(String collection) {
    config.mongoConfig.collection = collection;
    return this;
  }

  public MongoDBSourceBuilder isCapped(boolean isCapped) {
    config.isCapped = isCapped;
    return this;
  }

  public MongoDBSourceBuilder offsetField(String offset) {
    config.offsetField = offset;
    return this;
  }

  public MongoDBSourceBuilder initialOffset(String initialOffset) {
    config.initialOffset = initialOffset;
    return this;
  }

  public MongoDBSourceBuilder batchSize(int batchSize) {
    config.batchSize = batchSize;
    return this;
  }

  public MongoDBSourceBuilder maxBatchWaitTime(long maxBatchWaitTime) {
    config.maxBatchWaitTime = maxBatchWaitTime;
    return this;
  }

  public MongoDBSourceBuilder readPreference(ReadPreferenceLabel readPreferenceLabel) {
    config.readPreference = readPreferenceLabel;
    return this;
  }

  public MongoDBSource build() {
    return new MongoDBSource(config);
  }
}
