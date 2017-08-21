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
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

@SuppressWarnings("unchecked")
public abstract class AbstractMongoDBSourceBuilder<S extends AbstractMongoDBSource, B extends AbstractMongoDBSourceBuilder> {
  protected MongoSourceConfigBean config;

  protected AbstractMongoDBSourceBuilder() {
    config = new MongoSourceConfigBean();
    config.mongoConfig = new MongoDBConfig();
    config.mongoConfig.connectionString = "mongodb://";
    config.mongoConfig.collection = "collection";
    config.mongoConfig.authenticationType = AuthenticationType.NONE;
    config.mongoConfig.username = null;
    config.mongoConfig.password = null;
    config.batchSize = 1000;
    config.maxBatchWaitTime = 1;
    config.readPreference = ReadPreferenceLabel.NEAREST;
    config.isCapped = true;
  }

  public B connectionString(String connectionString) {
    config.mongoConfig.connectionString = connectionString;
    return (B)this;
  }

  public B collection(String collection) {
    config.mongoConfig.collection = collection;
    return (B)this;
  }

  public B batchSize(int batchSize) {
    config.batchSize = batchSize;
    return (B)this;
  }

  public B maxBatchWaitTime(long maxBatchWaitTime) {
    config.maxBatchWaitTime = maxBatchWaitTime;
    return (B)this;
  }

  public B readPreference(ReadPreferenceLabel readPreferenceLabel) {
    config.readPreference = readPreferenceLabel;
    return (B)this;
  }

  public abstract S build();
}
